// object.c — Content-addressable object store

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>

// ─── PROVIDED ──────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);
    EVP_MD_CTX_free(ctx);
}

void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ─── IMPLEMENTATION ────────────────────────────────────────────

int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    const char *type_str;

    switch (type) {
        case OBJ_BLOB:
            type_str = "blob";
            break;
        case OBJ_TREE:
            type_str = "tree";
            break;
        case OBJ_COMMIT:
            type_str = "commit";
            break;
        default:
            return -1;
    }

    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len);
    if (header_len < 0 || header_len >= (int)sizeof(header)) {
        return -1;
    }

    size_t total_size = (size_t)header_len + 1 + len;

    unsigned char *buffer = malloc(total_size);
    if (!buffer) {
        return -1;
    }

    memcpy(buffer, header, (size_t)header_len);
    buffer[header_len] = '\0';
    if (len > 0) {
        memcpy(buffer + header_len + 1, data, len);
    }

    compute_hash(buffer, total_size, id_out);

    if (object_exists(id_out)) {
        free(buffer);
        return 0;
    }

    char path[512];
    object_path(id_out, path, sizeof(path));

    char dir[512];
    strncpy(dir, path, sizeof(dir) - 1);
    dir[sizeof(dir) - 1] = '\0';

    char *slash = strrchr(dir, '/');
    if (!slash) {
        free(buffer);
        return -1;
    }
    *slash = '\0';

    mkdir(OBJECTS_DIR, 0755);
    mkdir(dir, 0755);

    char temp_path[512];
    int ret = snprintf(temp_path, sizeof(temp_path), "%s.tmp", path);
    if (ret < 0 || ret >= (int)sizeof(temp_path)) {
        free(buffer);
        return -1;
    }

    int fd = open(temp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        free(buffer);
        return -1;
    }

    ssize_t written = write(fd, buffer, total_size);
    if (written != (ssize_t)total_size) {
        close(fd);
        unlink(temp_path);
        free(buffer);
        return -1;
    }

    if (fsync(fd) != 0) {
        close(fd);
        unlink(temp_path);
        free(buffer);
        return -1;
    }

    if (close(fd) != 0) {
        unlink(temp_path);
        free(buffer);
        return -1;
    }

    if (rename(temp_path, path) != 0) {
        unlink(temp_path);
        free(buffer);
        return -1;
    }

    int dir_fd = open(dir, O_RDONLY);
    if (dir_fd >= 0) {
        fsync(dir_fd);
        close(dir_fd);
    }

    free(buffer);
    return 0;
}

int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    char path[512];
    object_path(id, path, sizeof(path));

    FILE *fp = fopen(path, "rb");
    if (!fp) {
        return -1;
    }

    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp);
        return -1;
    }

    long file_size = ftell(fp);
    if (file_size <= 0) {
        fclose(fp);
        return -1;
    }

    rewind(fp);

    unsigned char *buffer = malloc((size_t)file_size);
    if (!buffer) {
        fclose(fp);
        return -1;
    }

    if (fread(buffer, 1, (size_t)file_size, fp) != (size_t)file_size) {
        fclose(fp);
        free(buffer);
        return -1;
    }

    fclose(fp);

    ObjectID computed;
    compute_hash(buffer, (size_t)file_size, &computed);

    if (memcmp(computed.hash, id->hash, HASH_SIZE) != 0) {
        free(buffer);
        return -1;
    }

    unsigned char *null_pos = memchr(buffer, '\0', (size_t)file_size);
    if (!null_pos) {
        free(buffer);
        return -1;
    }

    char type_str[16];
    size_t size;

    if (sscanf((char *)buffer, "%15s %zu", type_str, &size) != 2) {
        free(buffer);
        return -1;
    }

    if (strcmp(type_str, "blob") == 0) {
        *type_out = OBJ_BLOB;
    } else if (strcmp(type_str, "tree") == 0) {
        *type_out = OBJ_TREE;
    } else if (strcmp(type_str, "commit") == 0) {
        *type_out = OBJ_COMMIT;
    } else {
        free(buffer);
        return -1;
    }

    size_t actual_data_size = (size_t)file_size - (size_t)((null_pos + 1) - buffer);
    if (size != actual_data_size) {
        free(buffer);
        return -1;
    }

    *data_out = malloc(size);
    if (!*data_out) {
        free(buffer);
        return -1;
    }

    if (size > 0) {
        memcpy(*data_out, null_pos + 1, size);
    }
    *len_out = size;

    free(buffer);
    return 0;
}
