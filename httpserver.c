// Asgn 4 A Multi-Threaded HTTP Server
// By: Nam Tran

#include "asgn2_helper_funcs.h"
#include "connection.h"
#include "debug.h"
#include "response.h"
#include "request.h"
#include "queue.h"

#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/file.h>

#define OPTIONS "t:"

queue_t *q;
pthread_mutex_t lock;
pthread_mutex_t writer_lock;
pthread_mutex_t *ma;
int hash_size;
int *readers_count;
int *writers_count;
pthread_cond_t *rca;
pthread_cond_t *wca;

void handle_connection(int);
void handle_get(conn_t *);
void handle_put(conn_t *);
void handle_unsupported(conn_t *);
void log_audit(conn_t *conn, const Response_t *res);
void *worker_Thread();

// Hash function using the djb2 algorithm
int hash(const char *str, int size) {
    unsigned long hash = 5381;
    int c;

    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    }

    return hash % size;
}

int main(int argc, char **argv) {
    int opt = 0;
    int worker_threads = 4;
    while ((opt = getopt(argc, argv, OPTIONS)) != -1) {
        switch (opt) {
        case 't':
            worker_threads = atoi(optarg);
            if (worker_threads <= 0) {
                fprintf(stderr, "Invalid worker thread size\n");
                return EXIT_FAILURE;
            }
            break;
        default: break;
        }
    }

    if (argc < 2) {
        warnx("wrong arguments: %s port_num", argv[optind]);
        fprintf(stderr, "usage: %s <port>\n", argv[optind]);
        return EXIT_FAILURE;
    }

    if (argv[optind] == NULL) {
        fprintf(stderr, "No port input\n");
        return EXIT_FAILURE;
    }

    char *endptr = NULL;
    size_t port = (size_t) strtoull(argv[optind], &endptr, 10);

    if (endptr && *endptr != '\0') {
        warnx("invalid port number: %s", argv[optind]);
        return EXIT_FAILURE;
    }

    if ((optind + 1) < argc) {
        fprintf(stderr, "Too much arguments\n");
        return EXIT_FAILURE;
    }

    signal(SIGPIPE, SIG_IGN);

    Listener_Socket sock;
    if (listener_init(&sock, port)) {
        fprintf(stderr, "Port %zu already in use\n", port);
        return EXIT_FAILURE;
    }

    pthread_mutex_init(&lock, NULL);
    pthread_mutex_init(&writer_lock, NULL);
    pthread_t threads[worker_threads];
    q = queue_new(worker_threads);

    for (int i = 0; i < worker_threads; i++) {
        pthread_create(&(threads[i]), NULL, worker_Thread, NULL);
    }

    hash_size = worker_threads * 4;
    pthread_mutex_t mutex_array[hash_size];
    pthread_cond_t read_cond_array[hash_size];
    pthread_cond_t write_cond_array[hash_size];
    int readers_array[hash_size];
    int writers_array[hash_size];

    ma = mutex_array;
    rca = read_cond_array;
    wca = write_cond_array;
    readers_count = readers_array;
    writers_count = writers_array;

    for (int i = 0; i < hash_size; i++) {
        pthread_mutex_init(&ma[i], NULL);
        pthread_cond_init(&rca[i], NULL);
        pthread_cond_init(&wca[i], NULL);
        readers_count[i] = 0;
        writers_count[i] = 0;
    }

    while (1) {
        int dispatcher_connfd = listener_accept(&sock);
        queue_push(q, (void *) (intptr_t) dispatcher_connfd);
    }

    return EXIT_SUCCESS;
}

void *worker_Thread() {
    while (1) {
        int connfd;
        queue_pop(q, (void **) &connfd);

        if (connfd < 0) {
            continue;
        }

        handle_connection(connfd);
        close(connfd);
    }
}

void log_audit(conn_t *conn, const Response_t *res) {
    if (conn == NULL || res == NULL) {
        return;
    }
    char *header = conn_get_header(conn, "Request-Id");
    if (header == NULL) {
        header = "0";
    }
    fprintf(stderr, "%s,%s,%u,%s\n", request_get_str(conn_get_request(conn)), conn_get_uri(conn),
        response_get_code(res), header);
}

void handle_connection(int connfd) {

    conn_t *conn = conn_new(connfd);

    const Response_t *res = conn_parse(conn);

    if (res != NULL) {
        log_audit(conn, res);
        conn_send_response(conn, res);
    } else {
        //debug("%s", conn_str(conn));
        const Request_t *req = conn_get_request(conn);
        if (req == &REQUEST_GET) {
            handle_get(conn);
        } else if (req == &REQUEST_PUT) {
            handle_put(conn);
        } else {
            handle_unsupported(conn);
        }
    }

    conn_delete(&conn);
}

void handle_get(conn_t *conn) {
    const Response_t *res = NULL;
    char *uri = conn_get_uri(conn);

    pthread_mutex_lock(&ma[hash(uri, hash_size)]);
    if (writers_count[hash(uri, hash_size)] > 0) {
        pthread_cond_wait(&rca[hash(uri, hash_size)], &ma[hash(uri, hash_size)]);
    }
    readers_count[hash(uri, hash_size)] = readers_count[hash(uri, hash_size)] + 1;
    pthread_mutex_unlock(&ma[hash(uri, hash_size)]);

    pthread_mutex_lock(&ma[hash(uri, hash_size)]);

    //debug("GET request not implemented. But, we want to get %s", uri);

    // What are the steps in here?

    // 1. Open the file.
    // If  open it returns < 0, then use the result appropriately
    //   a. Cannot access -- use RESPONSE_FORBIDDEN
    //   b. Cannot find the file -- use RESPONSE_NOT_FOUND
    //   c. other error? -- use RESPONSE_INTERNAL_SERVER_ERROR
    // (hint: check errno for these cases)!
    //pthread_mutex_lock(&lock);
    //pthread_mutex_unlock(&lock);
    int fd = open(uri, O_RDONLY);
    if (fd < 0) {
        if (errno == EACCES) {
            res = &RESPONSE_FORBIDDEN;
        } else if (errno == ENOENT) {
            res = &RESPONSE_NOT_FOUND;
        } else {
            res = &RESPONSE_INTERNAL_SERVER_ERROR;
        }
        goto out;
    }
    // 2. Get the size of the file.
    // (hint: checkout the function fstat)!
    struct stat info;
    fstat(fd, &info);
    off_t file_size = info.st_size;

    // 3. Check if the file is a directory, because directories *will*
    // open, but are not valid.
    // (hint: checkout the macro "S_IFDIR", which you can use after you call fstat!)
    if (S_IFDIR == (info.st_mode & S_IFMT)) {
        res = &RESPONSE_FORBIDDEN;
        goto out;
    }

    // 4. Send the file
    // (hint: checkout the conn_send_file function!)

    if (conn_send_file(conn, fd, file_size) == NULL) {
        res = &RESPONSE_OK;
    }

    log_audit(conn, res);

    close(fd);

    pthread_mutex_unlock(&ma[hash(uri, hash_size)]);

    pthread_mutex_lock(&ma[hash(uri, hash_size)]);
    readers_count[hash(uri, hash_size)] = readers_count[hash(uri, hash_size)] - 1;
    pthread_mutex_unlock(&ma[hash(uri, hash_size)]);
    if (readers_count[hash(uri, hash_size)] == 0) {
        pthread_cond_signal(&wca[hash(uri, hash_size)]);
    } else {
        pthread_cond_broadcast(&rca[hash(uri, hash_size)]);
    }
    return;

out:
    log_audit(conn, res);
    conn_send_response(conn, res);
    close(fd);

    pthread_mutex_unlock(&ma[hash(uri, hash_size)]);

    pthread_mutex_lock(&ma[hash(uri, hash_size)]);
    readers_count[hash(uri, hash_size)] = readers_count[hash(uri, hash_size)] - 1;
    pthread_mutex_unlock(&ma[hash(uri, hash_size)]);
    if (readers_count[hash(uri, hash_size)] == 0) {
        pthread_cond_broadcast(&wca[hash(uri, hash_size)]);
    } else {
        pthread_cond_broadcast(&rca[hash(uri, hash_size)]);
    }
    return;
}

void handle_unsupported(conn_t *conn) {

    const Response_t *res = &RESPONSE_NOT_IMPLEMENTED;
    log_audit(conn, res);
    conn_send_response(conn, res);
}

void handle_put(conn_t *conn) {

    char *uri = conn_get_uri(conn);
    const Response_t *res = NULL;

    pthread_mutex_lock(&ma[hash(uri, hash_size)]);
    writers_count[hash(uri, hash_size)] = writers_count[hash(uri, hash_size)] + 1;
    if (readers_count[hash(uri, hash_size)] > 0 || writers_count[hash(uri, hash_size)] > 1) {
        pthread_cond_wait(&wca[hash(uri, hash_size)], &ma[hash(uri, hash_size)]);
    }
    pthread_mutex_unlock(&ma[hash(uri, hash_size)]);

    pthread_mutex_lock(&ma[hash(uri, hash_size)]);
    // Check if file already exists before opening it.
    int fd;
    bool existed = access(uri, F_OK) == 0;
    if (existed == true) {
        fd = open(uri, O_WRONLY, 0);
    } else {
        fd = open(uri, O_CREAT | O_WRONLY, 0600);
    }

    // Open the file..
    if (fd < 0) {
        debug("%s: %d", uri, errno);
        if (errno == EACCES || errno == EISDIR || errno == ENOENT) {
            res = &RESPONSE_FORBIDDEN;
            goto out;
        } else {
            res = &RESPONSE_INTERNAL_SERVER_ERROR;
            goto out;
        }
    }
    ftruncate(fd, 0);

    res = conn_recv_file(conn, fd);

    if (res == NULL && existed) {
        res = &RESPONSE_OK;
    } else if (res == NULL && !existed) {
        res = &RESPONSE_CREATED;
    }

out:
    log_audit(conn, res);
    conn_send_response(conn, res);
    close(fd);
    pthread_mutex_unlock(&ma[hash(uri, hash_size)]);

    pthread_mutex_lock(&ma[hash(uri, hash_size)]);
    writers_count[hash(uri, hash_size)] = writers_count[hash(uri, hash_size)] - 1;
    pthread_mutex_unlock(&ma[hash(uri, hash_size)]);
    if (writers_count[hash(uri, hash_size)] == 0) {
        pthread_cond_broadcast(&rca[hash(uri, hash_size)]);
    } else {
        pthread_cond_signal(&wca[hash(uri, hash_size)]);
    }
}
