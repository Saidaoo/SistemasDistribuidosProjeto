#ifndef BBS_CLIENT_H
#define BBS_CLIENT_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>

// ZeroMQ e MessagePack
#include <zmq.h>
#include <msgpack.h>

#define MAX_BUFFER 1024
#define SERVER_HOST "servidor"
#define SERVER_PORT "5555"

typedef struct {
    char current_user[50];
    void *context;
    void *req_socket;
    void *sub_socket;
    int listening;
    pthread_t listener_thread;
} BBSClient;

// Funções principais
int bbs_client_init(BBSClient *client, const char *server_host, const char *server_port);
void bbs_client_cleanup(BBSClient *client);
int bbs_client_login(BBSClient *client, const char *username);
int bbs_client_list_users(BBSClient *client);
int bbs_client_list_channels(BBSClient *client);
int bbs_client_create_channel(BBSClient *client, const char *channel_name);
int bbs_client_subscribe_channel(BBSClient *client, const char *channel_name);
int bbs_client_publish_message(BBSClient *client, const char *channel, const char *message);
int bbs_client_send_direct_message(BBSClient *client, const char *target_user, const char *message);

// Interface do usuário
void bbs_client_show_welcome();
void bbs_client_show_menu(const char *username);
void bbs_client_interactive_mode(BBSClient *client);

// Utilitários
char* get_current_timestamp();
void trim_newline(char *str);

int send_messagepack_request(BBSClient *client, const char *service, msgpack_sbuffer *sbuf);
int receive_messagepack_response(BBSClient *client, msgpack_object *response);
void process_direct_message(msgpack_object obj, const char *current_user);
void process_channel_message(msgpack_object obj, const char *current_user);
#endif