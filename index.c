// index.c — Staging area implementation
//
// Text format of .pes/index (one entry per line, sorted by path):
//
//   <mode-octal> <64-char-hex-hash> <mtime-seconds> <size> <path>

#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

// Forward declarations from object.c
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);
int hex_to_hash(const char *hex, ObjectID *id_out);
void hash_to_hex(const ObjectID *id, char *hex_out);

// ─── PROVIDED ────────────────────────────────────────────────────────────────

IndexEntry* index_find(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0)
            return &index->entries[i];
    }
    return NULL;
}

int index_remove(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0) {
            int remaining = index->count - i - 1;
            if (remaining > 0)
                memmove(&index->entries[i], &index->entries[i + 1],
                        remaining * sizeof(IndexEntry));
            index->count--;
            return index_save(index);
        }
    }
    fprintf(stderr, "error: '%s' is not in the index\n", path);
    return -1;
}

int index_status(const Index *index) {
    printf("Staged changes:\n");
    int staged_count = 0;
    for (int i = 0; i < index->count; i++) {
        printf("  staged:     %s\n", index->entries[i].path);
        staged_count++;
    }
    if (staged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Unstaged changes:\n");
    int unstaged_count = 0;
    for (int i = 0; i < index->count; i++) {
        struct stat st;
        if (stat(index->entries[i].path, &st) != 0) {
            printf("  deleted:    %s\n", index->entries[i].path);
            unstaged_count++;
        } else {
            if (st.st_mtime != (time_t)index->entries[i].mtime_sec ||
                st.st_size != (off_t)index->entries[i].size) {
                printf("  modified:   %s\n", index->entries[i].path);
                unstaged_count++;
            }
        }
    }
    if (unstaged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Untracked files:\n");
    int untracked_count = 0;
    DIR *dir = opendir(".");
    if (dir) {
        struct dirent *ent;
        while ((ent = readdir(dir)) != NULL) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
            if (strcmp(ent->d_name, ".pes") == 0) continue;
            if (strcmp(ent->d_name, "pes") == 0) continue;
            if (strstr(ent->d_name, ".o") != NULL) continue;

            int is_tracked = 0;
            for (int i = 0; i < index->count; i++) {
                if (strcmp(index->entries[i].path, ent->d_name) == 0) {
                    is_tracked = 1;
                    break;
                }
            }

            if (!is_tracked) {
                struct stat st;
                if (stat(ent->d_name, &st) == 0 && S_ISREG(st.st_mode)) {
                    printf("  untracked:  %s\n", ent->d_name);
                    untracked_count++;
                }
            }
        }
        closedir(dir);
    }
    if (untracked_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    return 0;
}

// ─── IMPLEMENTATION ──────────────────────────────────────────────────────────

static int compare_index_entries(const void *a, const void *b) {
    const IndexEntry *ea = (const IndexEntry *)a;
    const IndexEntry *eb = (const IndexEntry *)b;
    return strcmp(ea->path, eb->path);
}

int index_load(Index *index) {
    index->count = 0;

    FILE *fp = fopen(".pes/index", "r");
    if (!fp) {
        return 0;
    }

    char line[2048];

    while (fgets(line, sizeof(line), fp) != NULL) {
        if (index->count >= MAX_INDEX_ENTRIES) {
            fclose(fp);
            return -1;
        }

        IndexEntry *e = &index->entries[index->count];
        char hash_hex[HASH_HEX_SIZE + 1];
        unsigned int mode_tmp;
        unsigned long long mtime_tmp;
        unsigned int size_tmp;
        char path_tmp[512];

        int n = sscanf(line, "%o %64s %llu %u %511[^\n]",
                       &mode_tmp, hash_hex, &mtime_tmp, &size_tmp, path_tmp);
        if (n != 5) {
            fclose(fp);
            return -1;
        }

        e->mode = (uint32_t)mode_tmp;
        e->mtime_sec = (uint64_t)mtime_tmp;
        e->size = (uint32_t)size_tmp;

        strncpy(e->path, path_tmp, sizeof(e->path) - 1);
        e->path[sizeof(e->path) - 1] = '\0';

        if (hex_to_hash(hash_hex, &e->hash) != 0) {
            fclose(fp);
            return -1;
        }

        index->count++;
    }

    fclose(fp);
    return 0;
}

int index_save(const Index *index) {
    mkdir(".pes", 0755);

    Index sorted = *index;
    qsort(sorted.entries, sorted.count, sizeof(IndexEntry), compare_index_entries);

    FILE *fp = fopen(".pes/index.tmp", "w");
    if (!fp) return -1;

    for (int i = 0; i < sorted.count; i++) {
        char hash_hex[HASH_HEX_SIZE + 1];
        hash_to_hex(&sorted.entries[i].hash, hash_hex);

        if (fprintf(fp, "%o %s %llu %u %s\n",
                    sorted.entries[i].mode,
                    hash_hex,
                    (unsigned long long)sorted.entries[i].mtime_sec,
                    sorted.entries[i].size,
                    sorted.entries[i].path) < 0) {
            fclose(fp);
            unlink(".pes/index.tmp");
            return -1;
        }
    }

    fflush(fp);
    int fd = fileno(fp);
    if (fd < 0 || fsync(fd) != 0) {
        fclose(fp);
        unlink(".pes/index.tmp");
        return -1;
    }

    if (fclose(fp) != 0) {
        unlink(".pes/index.tmp");
        return -1;
    }

    if (rename(".pes/index.tmp", ".pes/index") != 0) {
        unlink(".pes/index.tmp");
        return -1;
    }

    int dirfd = open(".pes", O_RDONLY);
    if (dirfd >= 0) {
        fsync(dirfd);
        close(dirfd);
    }

    return 0;
}

int index_add(Index *index, const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) {
        fprintf(stderr, "error: cannot stat '%s'\n", path);
        return -1;
    }

    if (!S_ISREG(st.st_mode)) {
        fprintf(stderr, "error: '%s' is not a regular file\n", path);
        return -1;
    }

    FILE *fp = fopen(path, "rb");
    if (!fp) {
        fprintf(stderr, "error: cannot open '%s'\n", path);
        return -1;
    }

    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp);
        return -1;
    }

    long file_size = ftell(fp);
    if (file_size < 0) {
        fclose(fp);
        return -1;
    }

    rewind(fp);

    void *buffer = NULL;
    if (file_size > 0) {
        buffer = malloc((size_t)file_size);
        if (!buffer) {
            fclose(fp);
            return -1;
        }
        if (fread(buffer, 1, (size_t)file_size, fp) != (size_t)file_size) {
            free(buffer);
            fclose(fp);
            return -1;
        }
    }

    fclose(fp);

    ObjectID blob_id;
    if (object_write(OBJ_BLOB, buffer, (size_t)file_size, &blob_id) != 0) {
        free(buffer);
        return -1;
    }

    free(buffer);

    IndexEntry *entry = index_find(index, path);
    if (!entry) {
        if (index->count >= MAX_INDEX_ENTRIES) return -1;
        entry = &index->entries[index->count++];
    }

    entry->mode = (st.st_mode & S_IXUSR) ? 0100755 : 0100644;
    entry->hash = blob_id;
    entry->mtime_sec = (uint64_t)st.st_mtime;
    entry->size = (uint32_t)st.st_size;
    strncpy(entry->path, path, sizeof(entry->path) - 1);
    entry->path[sizeof(entry->path) - 1] = '\0';

    return index_save(index);
}
