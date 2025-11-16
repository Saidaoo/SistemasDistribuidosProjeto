#ifndef CLIENTE_H
#define CLIENTE_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <zmq.h>
#include <msgpack.h>
#include <time.h>

#define MAX_MESSAGE_LENGTH 256


typedef struct {
    void *context;
    void *req_socket;
    void *sub_socket;
    char server_host[256];
    char server_port[10];
    char current_user[50];
    int listening;
    pthread_t listen_thread;
    uint64_t logical_clock;
} BBSClient;


int bbs_client_init(BBSClient *client, const char *server_host, const char *server_port);
void bbs_client_cleanup(BBSClient *client);
int bbs_client_login(BBSClient *client, const char *username);
int bbs_client_list_users(BBSClient *client);
int bbs_client_list_channels(BBSClient *client);
int bbs_client_create_channel(BBSClient *client, const char *channel_name);
int bbs_client_subscribe_channel(BBSClient *client, const char *channel_name);
int bbs_client_publish_message(BBSClient *client, const char *channel, const char *message);
int bbs_client_send_direct_message(BBSClient *client, const char *target_user, const char *message);
void bbs_client_start_listening(BBSClient *client);
void bbs_client_stop_listening(BBSClient *client);
void bbs_client_interactive_mode(BBSClient *client);


void get_current_timestamp(char *timestamp_buffer);
void bbs_client_show_menu();


void process_direct_message(msgpack_object data_obj, const char *current_user);
void process_channel_message(msgpack_object data_obj, const char *current_user);

#endif