#include "cliente.h"
#include <msgpack.h>

void get_current_timestamp(char *timestamp_buffer) {
    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    strftime(timestamp_buffer, 20, "%Y-%m-%d %H:%M:%S", t);
}


int send_messagepack_request(BBSClient *client, const char *service, msgpack_sbuffer *sbuf) {
    if (!client || !client->req_socket) {
        printf("Erro: Cliente não inicializado\n");
        return 0;
    }

    zmq_send(client->req_socket, sbuf->data, sbuf->size, 0);
    return 1;
}


int receive_messagepack_raw(BBSClient *client, void **out_buf, size_t *out_size) {
    zmq_msg_t msg;
    zmq_msg_init(&msg);
    
    if (zmq_msg_recv(&msg, client->req_socket, 0) < 0) {
        zmq_msg_close(&msg);
        return 0;
    }

    *out_size = zmq_msg_size(&msg);
    *out_buf = malloc(*out_size);
    memcpy(*out_buf, zmq_msg_data(&msg), *out_size);
    
    zmq_msg_close(&msg);
    return 1;
}


void bbs_client_show_menu() {
    printf("\n=== BBS Client Menu ===\n");
    printf("1. Listar usuários online\n");
    printf("2. Listar canais\n");
    printf("3. Criar canal\n");
    printf("4. Inscrever-se em canal\n");
    printf("5. Enviar mensagem para canal\n");
    printf("6. Enviar mensagem direta\n");
    printf("7. Sair\n");
    printf("Escolha uma opção: ");
}


int bbs_client_init(BBSClient *client, const char *broker_host, const char *broker_port) {
    memset(client, 0, sizeof(BBSClient));
    strncpy(client->server_host, broker_host, sizeof(client->server_host) - 1);
    strncpy(client->server_port, broker_port, sizeof(client->server_port) - 1);
    
    client->logical_clock = 0;

    client->context = zmq_ctx_new();
    if (!client->context) {
        printf("Erro ao criar contexto ZeroMQ\n");
        return 0;
    }
    

    client->req_socket = zmq_socket(client->context, ZMQ_REQ);
    if (!client->req_socket) {
        printf("Erro ao criar socket REQ: %s\n", zmq_strerror(errno));
        zmq_ctx_destroy(client->context);
        return 0;
    }
    
    int rcv_timeout = 5000;
    zmq_setsockopt(client->req_socket, ZMQ_RCVTIMEO, &rcv_timeout, sizeof(rcv_timeout));
    
    char req_endpoint[100];
    snprintf(req_endpoint, sizeof(req_endpoint), "tcp://%s:%s", broker_host, broker_port);
    
    if (zmq_connect(client->req_socket, req_endpoint) != 0) {
        printf("Erro ao conectar com broker %s: %s\n", req_endpoint, zmq_strerror(errno));
        zmq_close(client->req_socket);
        zmq_ctx_destroy(client->context);
        return 0;
    }
    
    printf("✅ REQ conectado ao broker: %s\n", req_endpoint);
    

    char *proxy_host = getenv("PROXY_HOST");
    char *proxy_port = getenv("PROXY_PORT");
    
    if (!proxy_host) proxy_host = "proxy";
    if (!proxy_port) proxy_port = "5558";
    
    client->sub_socket = zmq_socket(client->context, ZMQ_SUB);
    if (!client->sub_socket) {
        printf("Erro ao criar socket SUB: %s\n", zmq_strerror(errno));
        zmq_close(client->req_socket);
        zmq_ctx_destroy(client->context);
        return 0;
    }
    
    char sub_endpoint[100];
    snprintf(sub_endpoint, sizeof(sub_endpoint), "tcp://%s:%s", proxy_host, proxy_port);
    
    if (zmq_connect(client->sub_socket, sub_endpoint) != 0) {
        printf("Erro ao conectar com proxy %s: %s\n", sub_endpoint, zmq_strerror(errno));
        zmq_close(client->req_socket);
        zmq_close(client->sub_socket);
        zmq_ctx_destroy(client->context);
        return 0;
    }
    
    zmq_setsockopt(client->sub_socket, ZMQ_SUBSCRIBE, "", 0);
    
    printf("✅ SUB conectado ao proxy: %s\n", sub_endpoint);
    printf("📢 Você receberá mensagens via PUB/SUB\n");
    
    return 1;
}

void bbs_client_cleanup(BBSClient *client) {
    if (!client) return;
    
    if (client->listening) {
        client->listening = 0;
        if (client->listen_thread) {
            pthread_join(client->listen_thread, NULL);
        }
    }
    
    if (client->req_socket) zmq_close(client->req_socket);
    if (client->sub_socket) zmq_close(client->sub_socket);
    if (client->context) zmq_ctx_destroy(client->context);
    
    memset(client, 0, sizeof(BBSClient));
}

int bbs_client_login(BBSClient *client, const char *username) {
    msgpack_sbuffer sbuf;
    msgpack_packer pk;
    
    msgpack_sbuffer_init(&sbuf);
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);
    
    msgpack_pack_map(&pk, 2);
    
    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "service", 7);
    msgpack_pack_str(&pk, 5);
    msgpack_pack_str_body(&pk, "login", 5);
    
    msgpack_pack_str(&pk, 4);
    msgpack_pack_str_body(&pk, "data", 4);
    msgpack_pack_map(&pk, 3);
    
    msgpack_pack_str(&pk, 4);
    msgpack_pack_str_body(&pk, "user", 4);
    msgpack_pack_str(&pk, strlen(username));
    msgpack_pack_str_body(&pk, username, strlen(username));
    
    msgpack_pack_str(&pk, 9);
    msgpack_pack_str_body(&pk, "timestamp", 9);
    msgpack_pack_uint64(&pk, (uint64_t)time(NULL));

    client->logical_clock++;
    msgpack_pack_str(&pk, 5);
    msgpack_pack_str_body(&pk, "clock", 5);
    msgpack_pack_uint64(&pk, client->logical_clock);
    

    if (!send_messagepack_request(client, "LOGIN", &sbuf)) {
        msgpack_sbuffer_destroy(&sbuf);
        return 0;
    }
    

    void *response_data;
    size_t response_size;
    if (!receive_messagepack_raw(client, &response_data, &response_size)) {
        printf("Erro: Timeout ou falha na resposta\n");
        msgpack_sbuffer_destroy(&sbuf);
        return 0;
    }
    
    msgpack_unpacked result;
    msgpack_unpacked_init(&result);
    
    if (msgpack_unpack_next(&result, response_data, response_size, NULL)) {
        msgpack_object obj = result.data;

        if (obj.type == MSGPACK_OBJECT_MAP) {
            for (uint32_t i = 0; i < obj.via.map.size; i++) {
                msgpack_object key = obj.via.map.ptr[i].key;
                msgpack_object val = obj.via.map.ptr[i].val;
                
                if (key.type == MSGPACK_OBJECT_STR && 
                    strncmp(key.via.str.ptr, "data", key.via.str.size) == 0 &&
                    val.type == MSGPACK_OBJECT_MAP) {
                    
                    for (uint32_t j = 0; j < val.via.map.size; j++) {
                        msgpack_object data_key = val.via.map.ptr[j].key;
                        msgpack_object data_val = val.via.map.ptr[j].val;
                        
                        if (data_key.type == MSGPACK_OBJECT_STR &&
                            strncmp(data_key.via.str.ptr, "clock", data_key.via.str.size) == 0 &&
                            data_val.type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
                            
                            uint64_t received_clock = data_val.via.u64;
                            if (received_clock > client->logical_clock) {
                                client->logical_clock = received_clock;
                            }
                            client->logical_clock++;
                            
                            printf("🕒 Relógio lógico atualizado: %lu\n", client->logical_clock);
                        }
                        
                        if (data_key.type == MSGPACK_OBJECT_STR &&
                            strncmp(data_key.via.str.ptr, "status", data_key.via.str.size) == 0 &&
                            data_val.type == MSGPACK_OBJECT_STR &&
                            strncmp(data_val.via.str.ptr, "sucesso", data_val.via.str.size) == 0) {
                            
                            strncpy(client->current_user, username, sizeof(client->current_user) - 1);
                            msgpack_sbuffer_destroy(&sbuf);
                            free(response_data);
                            msgpack_unpacked_destroy(&result);
                            return 1;
                        }
                    }
                }
            }
        }
    }
    
    printf("Erro: Login falhou\n");
    msgpack_sbuffer_destroy(&sbuf);
    free(response_data);
    msgpack_unpacked_destroy(&result);
    return 0;
}

int bbs_client_list_users(BBSClient *client) {
    msgpack_sbuffer sbuf;
    msgpack_packer pk;
    
    msgpack_sbuffer_init(&sbuf);
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);
    
    // Incrementa clock ANTES de enviar
    client->logical_clock++;
    
    msgpack_pack_map(&pk, 2);
    
    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "service", 7);
    msgpack_pack_str(&pk, 5);
    msgpack_pack_str_body(&pk, "users", 5);
    
    msgpack_pack_str(&pk, 4);
    msgpack_pack_str_body(&pk, "data", 4);
    msgpack_pack_map(&pk, 1);
    
    // Adiciona clock na requisição
    msgpack_pack_str(&pk, 5);
    msgpack_pack_str_body(&pk, "clock", 5);
    msgpack_pack_uint64(&pk, client->logical_clock);
    
    if (!send_messagepack_request(client, "users", &sbuf)) {
        msgpack_sbuffer_destroy(&sbuf);
        return 0;
    }
    
    void *response_data;
    size_t response_size;
    if (!receive_messagepack_raw(client, &response_data, &response_size)) {
        printf("Erro: Timeout na resposta\n");
        msgpack_sbuffer_destroy(&sbuf);
        return 0;
    }
    
    // ⭐ ADICIONE ESTA PARTE - Atualiza clock ao receber resposta
    msgpack_unpacked result;
    msgpack_unpacked_init(&result);
    
    if (msgpack_unpack_next(&result, response_data, response_size, NULL)) {
        msgpack_object obj = result.data;
        
        if (obj.type == MSGPACK_OBJECT_MAP) {
            for (uint32_t i = 0; i < obj.via.map.size; i++) {
                msgpack_object key = obj.via.map.ptr[i].key;
                msgpack_object val = obj.via.map.ptr[i].val;
                
                if (key.type == MSGPACK_OBJECT_STR && 
                    strncmp(key.via.str.ptr, "data", key.via.str.size) == 0 &&
                    val.type == MSGPACK_OBJECT_MAP) {
                    
                    for (uint32_t j = 0; j < val.via.map.size; j++) {
                        msgpack_object data_key = val.via.map.ptr[j].key;
                        msgpack_object data_val = val.via.map.ptr[j].val;
                        
                        // Atualiza relógio lógico
                        if (data_key.type == MSGPACK_OBJECT_STR &&
                            strncmp(data_key.via.str.ptr, "clock", data_key.via.str.size) == 0 &&
                            data_val.type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
                            
                            uint64_t received_clock = data_val.via.u64;
                            if (received_clock > client->logical_clock) {
                                client->logical_clock = received_clock;
                            }
                            client->logical_clock++;
                            printf("🕒 Relógio lógico atualizado: %lu\n", client->logical_clock);
                        }
                    }
                }
            }
        }
    }
    
    printf("=== Usuários Online ===\n");
    printf("(Funcionalidade de parse será implementada)\n");
    
    msgpack_unpacked_destroy(&result);
    free(response_data);
    msgpack_sbuffer_destroy(&sbuf);
    return 1;
}

int bbs_client_list_channels(BBSClient *client) {
    msgpack_sbuffer sbuf;
    msgpack_packer pk;
    
    msgpack_sbuffer_init(&sbuf);
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);
    
    // Incrementa clock ANTES de enviar
    client->logical_clock++;
    
    msgpack_pack_map(&pk, 2);
    
    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "service", 7);
    msgpack_pack_str(&pk, 8);
    msgpack_pack_str_body(&pk, "channels", 8);
    
    msgpack_pack_str(&pk, 4);
    msgpack_pack_str_body(&pk, "data", 4);
    msgpack_pack_map(&pk, 1);
    
    // Adiciona clock
    msgpack_pack_str(&pk, 5);
    msgpack_pack_str_body(&pk, "clock", 5);
    msgpack_pack_uint64(&pk, client->logical_clock);
    
    if (!send_messagepack_request(client, "channels", &sbuf)) {
        msgpack_sbuffer_destroy(&sbuf);
        return 0;
    }
    
    void *response_data;
    size_t response_size;
    if (!receive_messagepack_raw(client, &response_data, &response_size)) {
        printf("Erro: Timeout na resposta\n");
        msgpack_sbuffer_destroy(&sbuf);
        return 0;
    }
    
    // ⭐ ATUALIZA CLOCK AO RECEBER
    msgpack_unpacked result;
    msgpack_unpacked_init(&result);
    
    if (msgpack_unpack_next(&result, response_data, response_size, NULL)) {
        msgpack_object obj = result.data;
        
        if (obj.type == MSGPACK_OBJECT_MAP) {
            for (uint32_t i = 0; i < obj.via.map.size; i++) {
                msgpack_object key = obj.via.map.ptr[i].key;
                msgpack_object val = obj.via.map.ptr[i].val;
                
                if (key.type == MSGPACK_OBJECT_STR && 
                    strncmp(key.via.str.ptr, "data", key.via.str.size) == 0 &&
                    val.type == MSGPACK_OBJECT_MAP) {
                    
                    for (uint32_t j = 0; j < val.via.map.size; j++) {
                        msgpack_object data_key = val.via.map.ptr[j].key;
                        msgpack_object data_val = val.via.map.ptr[j].val;
                        
                        if (data_key.type == MSGPACK_OBJECT_STR &&
                            strncmp(data_key.via.str.ptr, "clock", data_key.via.str.size) == 0 &&
                            data_val.type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
                            
                            uint64_t received_clock = data_val.via.u64;
                            if (received_clock > client->logical_clock) {
                                client->logical_clock = received_clock;
                            }
                            client->logical_clock++;
                            printf("🕒 Relógio lógico atualizado: %lu\n", client->logical_clock);
                        }
                    }
                }
            }
        }
    }
    
    printf("=== Canais Disponíveis ===\n");
    printf("- geral\n- tech\n- random\n");
    
    msgpack_unpacked_destroy(&result);
    free(response_data);
    msgpack_sbuffer_destroy(&sbuf);
    return 1;
}

int bbs_client_create_channel(BBSClient *client, const char *channel_name) {
    msgpack_sbuffer sbuf;
    msgpack_packer pk;
    
    msgpack_sbuffer_init(&sbuf);
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);
    
    // Incrementa clock ANTES de enviar
    client->logical_clock++;
    
    msgpack_pack_map(&pk, 2);
    
    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "service", 7);
    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "channel", 7);
    
    msgpack_pack_str(&pk, 4);
    msgpack_pack_str_body(&pk, "data", 4);
    msgpack_pack_map(&pk, 2);  // ⚠️ Mudou de 1 para 2 (channel + clock)
    
    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "channel", 7);
    msgpack_pack_str(&pk, strlen(channel_name));
    msgpack_pack_str_body(&pk, channel_name, strlen(channel_name));
    
    // Adiciona clock
    msgpack_pack_str(&pk, 5);
    msgpack_pack_str_body(&pk, "clock", 5);
    msgpack_pack_uint64(&pk, client->logical_clock);
    
    if (!send_messagepack_request(client, "channel", &sbuf)) {
        msgpack_sbuffer_destroy(&sbuf);
        return 0;
    }
    
    void *response_data;
    size_t response_size;
    if (!receive_messagepack_raw(client, &response_data, &response_size)) {
        printf("Erro: Timeout na resposta\n");
        msgpack_sbuffer_destroy(&sbuf);
        return 0;
    }
    
    // ⭐ ATUALIZA CLOCK AO RECEBER
    msgpack_unpacked result;
    msgpack_unpacked_init(&result);
    
    if (msgpack_unpack_next(&result, response_data, response_size, NULL)) {
        msgpack_object obj = result.data;
        
        if (obj.type == MSGPACK_OBJECT_MAP) {
            for (uint32_t i = 0; i < obj.via.map.size; i++) {
                msgpack_object key = obj.via.map.ptr[i].key;
                msgpack_object val = obj.via.map.ptr[i].val;
                
                if (key.type == MSGPACK_OBJECT_STR && 
                    strncmp(key.via.str.ptr, "data", key.via.str.size) == 0 &&
                    val.type == MSGPACK_OBJECT_MAP) {
                    
                    for (uint32_t j = 0; j < val.via.map.size; j++) {
                        msgpack_object data_key = val.via.map.ptr[j].key;
                        msgpack_object data_val = val.via.map.ptr[j].val;
                        
                        if (data_key.type == MSGPACK_OBJECT_STR &&
                            strncmp(data_key.via.str.ptr, "clock", data_key.via.str.size) == 0 &&
                            data_val.type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
                            
                            uint64_t received_clock = data_val.via.u64;
                            if (received_clock > client->logical_clock) {
                                client->logical_clock = received_clock;
                            }
                            client->logical_clock++;
                            printf("🕒 Relógio lógico atualizado: %lu\n", client->logical_clock);
                        }
                    }
                }
            }
        }
    }
    
    printf("✅ Canal '%s' criado!\n", channel_name);
    
    msgpack_unpacked_destroy(&result);
    free(response_data);
    msgpack_sbuffer_destroy(&sbuf);
    return 1;
}

int bbs_client_subscribe_channel(BBSClient *client, const char *channel_name) {
    printf("✅ Inscrito no canal: %s\n", channel_name);
    printf("💡 Você receberá mensagens via PUB/SUB!\n");
    return 1;
}

int bbs_client_publish_message(BBSClient *client, const char *channel, const char *message) {
    msgpack_sbuffer sbuf;
    msgpack_packer pk;
    
    msgpack_sbuffer_init(&sbuf);
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);
    
    client->logical_clock++;
    
    msgpack_pack_map(&pk, 2);
    
    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "service", 7);
    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "publish", 7);
    
    msgpack_pack_str(&pk, 4);
    msgpack_pack_str_body(&pk, "data", 4);
    msgpack_pack_map(&pk, 4);
    
    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "channel", 7);
    msgpack_pack_str(&pk, strlen(channel));
    msgpack_pack_str_body(&pk, channel, strlen(channel));
    
    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "message", 7);
    msgpack_pack_str(&pk, strlen(message));
    msgpack_pack_str_body(&pk, message, strlen(message));
    
    msgpack_pack_str(&pk, 4);
    msgpack_pack_str_body(&pk, "user", 4);
    msgpack_pack_str(&pk, strlen(client->current_user));
    msgpack_pack_str_body(&pk, client->current_user, strlen(client->current_user));
    
    msgpack_pack_str(&pk, 5);
    msgpack_pack_str_body(&pk, "clock", 5);
    msgpack_pack_uint64(&pk, client->logical_clock);
    
    if (!send_messagepack_request(client, "publish", &sbuf)) {
        msgpack_sbuffer_destroy(&sbuf);
        return 0;
    }
    
    void *response_data;
    size_t response_size;
    if (!receive_messagepack_raw(client, &response_data, &response_size)) {
        printf("Erro: Timeout na resposta\n");
        msgpack_sbuffer_destroy(&sbuf);
        return 0;
    }
    
    // ⭐ NOVO: Atualiza relógio lógico ao receber resposta
    msgpack_unpacked result;
    msgpack_unpacked_init(&result);
    
    if (msgpack_unpack_next(&result, response_data, response_size, NULL)) {
        msgpack_object obj = result.data;
        
        if (obj.type == MSGPACK_OBJECT_MAP) {
            for (uint32_t i = 0; i < obj.via.map.size; i++) {
                msgpack_object key = obj.via.map.ptr[i].key;
                msgpack_object val = obj.via.map.ptr[i].val;
                
                if (key.type == MSGPACK_OBJECT_STR && 
                    strncmp(key.via.str.ptr, "data", key.via.str.size) == 0 &&
                    val.type == MSGPACK_OBJECT_MAP) {
                    
                    for (uint32_t j = 0; j < val.via.map.size; j++) {
                        msgpack_object data_key = val.via.map.ptr[j].key;
                        msgpack_object data_val = val.via.map.ptr[j].val;
                        
                        if (data_key.type == MSGPACK_OBJECT_STR &&
                            strncmp(data_key.via.str.ptr, "clock", data_key.via.str.size) == 0 &&
                            data_val.type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
                            
                            uint64_t received_clock = data_val.via.u64;
                            if (received_clock > client->logical_clock) {
                                client->logical_clock = received_clock;
                            }
                            client->logical_clock++;
                            
                            printf("🕒 Relógio lógico atualizado: %lu\n", client->logical_clock);
                        }
                    }
                }
            }
        }
    }
    
    printf("✅ Mensagem enviada para '%s' (clock: %lu)\n", channel, client->logical_clock);
    
    msgpack_unpacked_destroy(&result);
    free(response_data);
    msgpack_sbuffer_destroy(&sbuf);
    return 1;
}

int bbs_client_send_direct_message(BBSClient *client, const char *target_user, const char *message) {
    msgpack_sbuffer sbuf;
    msgpack_packer pk;
    
    msgpack_sbuffer_init(&sbuf);
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);
    
    client->logical_clock++;
    
    msgpack_pack_map(&pk, 2);
    
    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "service", 7);
    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "message", 7);
    
    msgpack_pack_str(&pk, 4);
    msgpack_pack_str_body(&pk, "data", 4);
    msgpack_pack_map(&pk, 4);
    
    msgpack_pack_str(&pk, 3);
    msgpack_pack_str_body(&pk, "dst", 3);
    msgpack_pack_str(&pk, strlen(target_user));
    msgpack_pack_str_body(&pk, target_user, strlen(target_user));
    
    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "message", 7);
    msgpack_pack_str(&pk, strlen(message));
    msgpack_pack_str_body(&pk, message, strlen(message));
    
    msgpack_pack_str(&pk, 4);
    msgpack_pack_str_body(&pk, "from", 4);
    msgpack_pack_str(&pk, strlen(client->current_user));
    msgpack_pack_str_body(&pk, client->current_user, strlen(client->current_user));

    msgpack_pack_str(&pk, 5);
    msgpack_pack_str_body(&pk, "clock", 5);
    msgpack_pack_uint64(&pk, client->logical_clock);
    
    if (!send_messagepack_request(client, "message", &sbuf)) {
        msgpack_sbuffer_destroy(&sbuf);
        return 0;
    }
    
    void *response_data;
    size_t response_size;
    if (!receive_messagepack_raw(client, &response_data, &response_size)) {
        printf("Erro: Timeout na resposta\n");
        msgpack_sbuffer_destroy(&sbuf);
        return 0;
    }
    
    // ⭐ NOVO: Atualiza relógio lógico ao receber resposta
    msgpack_unpacked result;
    msgpack_unpacked_init(&result);
    
    if (msgpack_unpack_next(&result, response_data, response_size, NULL)) {
        msgpack_object obj = result.data;
        
        if (obj.type == MSGPACK_OBJECT_MAP) {
            for (uint32_t i = 0; i < obj.via.map.size; i++) {
                msgpack_object key = obj.via.map.ptr[i].key;
                msgpack_object val = obj.via.map.ptr[i].val;
                
                if (key.type == MSGPACK_OBJECT_STR && 
                    strncmp(key.via.str.ptr, "data", key.via.str.size) == 0 &&
                    val.type == MSGPACK_OBJECT_MAP) {
                    
                    for (uint32_t j = 0; j < val.via.map.size; j++) {
                        msgpack_object data_key = val.via.map.ptr[j].key;
                        msgpack_object data_val = val.via.map.ptr[j].val;
                        
                        if (data_key.type == MSGPACK_OBJECT_STR &&
                            strncmp(data_key.via.str.ptr, "clock", data_key.via.str.size) == 0 &&
                            data_val.type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
                            
                            uint64_t received_clock = data_val.via.u64;
                            if (received_clock > client->logical_clock) {
                                client->logical_clock = received_clock;
                            }
                            client->logical_clock++;
                            
                            printf("🕒 Relógio lógico atualizado: %lu\n", client->logical_clock);
                        }
                    }
                }
            }
        }
    }
    
    printf("✅ Mensagem enviada para %s (clock: %lu)\n", target_user, client->logical_clock);
    
    msgpack_unpacked_destroy(&result);
    free(response_data);
    msgpack_sbuffer_destroy(&sbuf);
    return 1;
}


void* listen_messages_thread(void *arg) {
    BBSClient *client = (BBSClient *)arg;
    printf("👂 Thread de escuta iniciada\n");
    
    while (client->listening) {
        char topic[256];
        char buffer[2048];
        
        // Recebe parte 1: Tópico/Canal (não-bloqueante)
        int topic_size = zmq_recv(client->sub_socket, topic, sizeof(topic) - 1, ZMQ_DONTWAIT);
        
        if (topic_size > 0) {
            topic[topic_size] = '\0';
            
            // Recebe parte 2: Dados MessagePack
            int msg_size = zmq_recv(client->sub_socket, buffer, sizeof(buffer), 0);
            
            if (msg_size > 0) {
                // Desserializa MessagePack
                msgpack_unpacked result;
                msgpack_unpacked_init(&result);
                
                if (msgpack_unpack_next(&result, buffer, msg_size, NULL)) {
                    msgpack_object obj = result.data;
                    
                    // Procura pelo campo "data" -> "message"
                    if (obj.type == MSGPACK_OBJECT_MAP) {
                        for (uint32_t i = 0; i < obj.via.map.size; i++) {
                            msgpack_object key = obj.via.map.ptr[i].key;
                            msgpack_object val = obj.via.map.ptr[i].val;
                            
                            // Procura "data"
                            if (key.type == MSGPACK_OBJECT_STR && 
                                strncmp(key.via.str.ptr, "data", key.via.str.size) == 0 &&
                                val.type == MSGPACK_OBJECT_MAP) {
                                
                                // Dentro de "data", procura "message"
                                for (uint32_t j = 0; j < val.via.map.size; j++) {
                                    msgpack_object data_key = val.via.map.ptr[j].key;
                                    msgpack_object data_val = val.via.map.ptr[j].val;
                                    
                                    if (data_key.type == MSGPACK_OBJECT_STR &&
                                        strncmp(data_key.via.str.ptr, "message", data_key.via.str.size) == 0 &&
                                        data_val.type == MSGPACK_OBJECT_STR) {
                                        
                                        // Mostra a mensagem formatada
                                        printf("\n📢 [%s] %.*s\n", 
                                               topic,
                                               (int)data_val.via.str.size, 
                                               data_val.via.str.ptr);
                                        printf("Escolha uma opção: ");
                                        fflush(stdout);
                                        break;
                                    }
                                }
                                break;
                            }
                        }
                    }
                }
                
                msgpack_unpacked_destroy(&result);
            }
        }
        
        // Pequena pausa
        usleep(100000); // 100ms
    }
    
    return NULL;
}

// Iniciar escuta
void bbs_client_start_listening(BBSClient *client) {
    client->listening = 1;
    pthread_create(&client->listen_thread, NULL, listen_messages_thread, client);
}

// Parar escuta
void bbs_client_stop_listening(BBSClient *client) {
    client->listening = 0;
    if (client->listen_thread) {
        pthread_join(client->listen_thread, NULL);
    }
}

// Modo interativo
void bbs_client_interactive_mode(BBSClient *client) {
    bbs_client_start_listening(client);
    
    int running = 1;
    while (running) {
        bbs_client_show_menu();
        
        int choice;
        if (scanf("%d", &choice) != 1) {
            while (getchar() != '\n');
            printf("Opção inválida!\n");
            continue;
        }
        
        switch (choice) {
            case 1:
                bbs_client_list_users(client);
                break;
            case 2:
                bbs_client_list_channels(client);
                break;
            case 3: {
                char channel[50];
                printf("Nome do canal: ");
                scanf("%49s", channel);
                bbs_client_create_channel(client, channel);
                break;
            }
            case 4: {
                char channel[50];
                printf("Nome do canal: ");
                scanf("%49s", channel);
                bbs_client_subscribe_channel(client, channel);
                break;
            }
            case 5: {
                char channel[50], message[256];
                printf("Canal: ");
                scanf("%49s", channel);
                printf("Mensagem: ");
                getchar();
                fgets(message, sizeof(message), stdin);
                message[strcspn(message, "\n")] = 0;
                bbs_client_publish_message(client, channel, message);
                break;
            }
            case 6: {
                char user[50], message[256];
                printf("Usuário: ");
                scanf("%49s", user);
                printf("Mensagem: ");
                getchar();
                fgets(message, sizeof(message), stdin);
                message[strcspn(message, "\n")] = 0;
                bbs_client_send_direct_message(client, user, message);
                break;
            }
            case 7:
                running = 0;
                printf("Saindo...\n");
                break;
            default:
                printf("Opção inválida!\n");
        }
    }
    
    bbs_client_stop_listening(client);
}


int main(int argc, char *argv[]) {

    char *broker_host = getenv("BROKER_HOST");
    char *broker_port = getenv("BROKER_PORT");
    
    if (!broker_host) broker_host = "broker";
    if (!broker_port) broker_port = "5550";
    
    if (argc >= 3) {
        broker_host = argv[1];
        broker_port = argv[2];
    }
    
    BBSClient client;
    char username[50];
    
    printf("======================================\n");
    printf("         Sistemas Distribuídos        \n");
    printf("======================================\n");
    printf("📡 Conectando ao broker: %s:%s\n", broker_host, broker_port);
    printf("   (Balanceamento entre 3 servidores)\n");
    printf("======================================\n\n");
    

    if (!bbs_client_init(&client, broker_host, broker_port)) {
        printf("❌ Erro: Não foi possível conectar ao broker\n");
        return 1;
    }
    
    printf("\n✅ Cliente inicializado com sucesso!\n");
    printf("🔄 REQ/REP: Comandos via broker\n");
    printf("📢 PUB/SUB: Mensagens via proxy\n\n");
    

    printf("Digite seu nome de usuário: ");
    if (fgets(username, sizeof(username), stdin) == NULL) {
        bbs_client_cleanup(&client);
        return 1;
    }
    
    username[strcspn(username, "\n")] = 0;
    
    if (strlen(username) == 0) {
        strcpy(username, "UsuarioAnonimo");
    }
    
    printf("\n🔐 Fazendo login como: %s\n", username);
    
    if (bbs_client_login(&client, username)) {
        printf("✅ Login realizado!\n");
        printf("💡 Bots ativos: Bot_Alice, Bot_Bob\n\n");
    } else {
        printf("❌ Erro no login\n");
        bbs_client_cleanup(&client);
        return 1;
    }
    
    bbs_client_interactive_mode(&client);
    
    bbs_client_cleanup(&client);
    printf("\n👋 Cliente finalizado.\n");
    
    return 0;
}