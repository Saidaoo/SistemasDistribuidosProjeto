#include "cliente.h"
#include <unistd.h>
#include <msgpack.h>

static volatile int keep_listening = 0;

// Função auxiliar para enviar requisições MessagePack
int send_messagepack_request(BBSClient *client, const char *service, msgpack_sbuffer *sbuf) {
    zmq_msg_t message;
    zmq_msg_init_size(&message, sbuf->size);
    memcpy(zmq_msg_data(&message), sbuf->data, sbuf->size);
    
    printf("📤 Enviando requisição %s (%zu bytes)\n", service, sbuf->size);
    
    if (zmq_msg_send(&message, client->req_socket, 0) == -1) {
        fprintf(stderr, "❌ Erro ao enviar mensagem: %s\n", zmq_strerror(errno));
        zmq_msg_close(&message);
        return -1;
    }
    
    zmq_msg_close(&message);
    return 0;
}

// Função auxiliar para receber respostas
int receive_messagepack_response(BBSClient *client, msgpack_object *response) {
    zmq_msg_t response_msg;
    zmq_msg_init(&response_msg);
    
    if (zmq_msg_recv(&response_msg, client->req_socket, 0) == -1) {
        fprintf(stderr, "❌ Erro ao receber resposta: %s\n", zmq_strerror(errno));
        zmq_msg_close(&response_msg);
        return -1;
    }
    
    size_t response_size = zmq_msg_size(&response_msg);
    void *response_data = zmq_msg_data(&response_msg);
    
    // Desempacota resposta MessagePack
    msgpack_unpacked unpacked;
    msgpack_unpacked_init(&unpacked);
    
    size_t offset = 0;
    if (!msgpack_unpack_next(&unpacked, response_data, response_size, &offset)) {
        fprintf(stderr, "❌ Erro ao desempacotar resposta\n");
        msgpack_unpacked_destroy(&unpacked);
        zmq_msg_close(&response_msg);
        return -1;
    }
    
    *response = unpacked.data;
    printf("✅ Resposta recebida (%zu bytes)\n", response_size);
    
    zmq_msg_close(&response_msg);
    msgpack_unpacked_destroy(&unpacked);
    return 0;
}

// Função auxiliar para extrair dados da resposta
int extract_string_array_from_response(msgpack_object response, const char *field_name, char ***array, int *count) {
    if (response.type == MSGPACK_OBJECT_MAP) {
        for (uint32_t i = 0; i < response.via.map.size; i++) {
            msgpack_object_kv kv = response.via.map.ptr[i];
            if (kv.key.type == MSGPACK_OBJECT_STR && 
                strncmp(kv.key.via.str.ptr, "data", kv.key.via.str.size) == 0) {
                
                if (kv.val.type == MSGPACK_OBJECT_MAP) {
                    for (uint32_t j = 0; j < kv.val.via.map.size; j++) {
                        msgpack_object_kv data_kv = kv.val.via.map.ptr[j];
                        if (data_kv.key.type == MSGPACK_OBJECT_STR &&
                            strncmp(data_kv.key.via.str.ptr, field_name, data_kv.key.via.str.size) == 0) {
                            
                            if (data_kv.val.type == MSGPACK_OBJECT_ARRAY) {
                                *count = data_kv.val.via.array.size;
                                *array = malloc(*count * sizeof(char*));
                                
                                for (int k = 0; k < *count; k++) {
                                    msgpack_object item = data_kv.val.via.array.ptr[k];
                                    if (item.type == MSGPACK_OBJECT_STR) {
                                        (*array)[k] = malloc(item.via.str.size + 1);
                                        strncpy((*array)[k], item.via.str.ptr, item.via.str.size);
                                        (*array)[k][item.via.str.size] = '\0';
                                    } else {
                                        (*array)[k] = strdup("(inválido)");
                                    }
                                }
                                return 1;
                            }
                        }
                    }
                }
            }
        }
    }
    return 0;
}

int bbs_client_init(BBSClient *client, const char *server_host, const char *server_port) {
    printf("🚀 Inicializando cliente BBS em C...\n");
    
    // Inicializa ZeroMQ
    client->context = zmq_ctx_new();
    if (!client->context) {
        fprintf(stderr, "❌ Erro ao criar contexto ZeroMQ\n");
        return -1;
    }

    // Socket REQ para servidor
    client->req_socket = zmq_socket(client->context, ZMQ_REQ);
    if (!client->req_socket) {
        fprintf(stderr, "❌ Erro ao criar socket REQ: %s\n", zmq_strerror(errno));
        zmq_ctx_destroy(client->context);
        return -1;
    }

    char req_endpoint[256];
    snprintf(req_endpoint, sizeof(req_endpoint), "tcp://%s:%s", SERVER_HOST, SERVER_PORT);
    
    printf("🔌 Conectando em: %s\n", req_endpoint);
    if (zmq_connect(client->req_socket, req_endpoint) != 0) {
        fprintf(stderr, "❌ Erro ao conectar no servidor: %s\n", zmq_strerror(errno));
        zmq_close(client->req_socket);
        zmq_ctx_destroy(client->context);
        return -1;
    }

    // Socket SUB para receber mensagens
    client->sub_socket = zmq_socket(client->context, ZMQ_SUB);
    if (!client->sub_socket) {
        fprintf(stderr, "❌ Erro ao criar socket SUB: %s\n", zmq_strerror(errno));
        zmq_close(client->req_socket);
        zmq_ctx_destroy(client->context);
        return -1;
    }

    if (zmq_connect(client->sub_socket, "tcp://proxy:5558") != 0) {
        fprintf(stderr, "❌ Erro ao conectar no proxy: %s\n", zmq_strerror(errno));
        zmq_close(client->sub_socket);
        zmq_close(client->req_socket);
        zmq_ctx_destroy(client->context);
        return -1;
    }

    client->listening = 0;
    memset(client->current_user, 0, sizeof(client->current_user));
    
    printf("✅ Cliente BBS em C inicializado com sucesso!\n");
    return 0;
}

void bbs_client_cleanup(BBSClient *client) {
    printf("🧹 Finalizando cliente...\n");
    keep_listening = 0;
    client->listening = 0;
    
    if (client->listener_thread) {
        pthread_join(client->listener_thread, NULL);
    }
    
    if (client->sub_socket) zmq_close(client->sub_socket);
    if (client->req_socket) zmq_close(client->req_socket);
    if (client->context) zmq_ctx_destroy(client->context);
    
    printf("👋 Cliente BBS finalizado\n");
}

char* get_current_timestamp() {
    static char buffer[64];
    time_t now = time(NULL);
    struct tm *tm_info = localtime(&now);
    
    strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%S", tm_info);
    return buffer;
}

void trim_newline(char *str) {
    int len = strlen(str);
    if (len > 0 && str[len-1] == '\n') {
        str[len-1] = '\0';
    }
}

int bbs_client_login(BBSClient *client, const char *username) {
    printf("🔐 Tentando login REAL como: %s\n", username);
    
    // Prepara dados do login
    msgpack_sbuffer sbuf;
    msgpack_packer pk;
    
    msgpack_sbuffer_init(&sbuf);
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);
    
    // Empacota: {service: "login", data: {user: "...", timestamp: "..."}}
    msgpack_pack_map(&pk, 2);
    
    // service
    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "service", 7);
    msgpack_pack_str(&pk, 5);
    msgpack_pack_str_body(&pk, "login", 5);
    
    // data
    msgpack_pack_str(&pk, 4);
    msgpack_pack_str_body(&pk, "data", 4);
    msgpack_pack_map(&pk, 2);
    
    // user
    msgpack_pack_str(&pk, 4);
    msgpack_pack_str_body(&pk, "user", 4);
    msgpack_pack_str(&pk, strlen(username));
    msgpack_pack_str_body(&pk, username, strlen(username));
    
    // timestamp
    msgpack_pack_str(&pk, 9);
    msgpack_pack_str_body(&pk, "timestamp", 9);
    char *timestamp = get_current_timestamp();
    msgpack_pack_str(&pk, strlen(timestamp));
    msgpack_pack_str_body(&pk, timestamp, strlen(timestamp));
    
    // Envia requisição
    if (send_messagepack_request(client, "login", &sbuf) != 0) {
        msgpack_sbuffer_destroy(&sbuf);
        return 0;
    }
    
    // Recebe resposta
    msgpack_object response;
    if (receive_messagepack_response(client, &response) != 0) {
        msgpack_sbuffer_destroy(&sbuf);
        return 0;
    }
    
    // Processa resposta
    if (response.type == MSGPACK_OBJECT_MAP) {
        for (uint32_t i = 0; i < response.via.map.size; i++) {
            msgpack_object_kv kv = response.via.map.ptr[i];
            if (kv.key.type == MSGPACK_OBJECT_STR && 
                strncmp(kv.key.via.str.ptr, "data", kv.key.via.str.size) == 0) {
                
                if (kv.val.type == MSGPACK_OBJECT_MAP) {
                    for (uint32_t j = 0; j < kv.val.via.map.size; j++) {
                        msgpack_object_kv data_kv = kv.val.via.map.ptr[j];
                        if (data_kv.key.type == MSGPACK_OBJECT_STR &&
                            strncmp(data_kv.key.via.str.ptr, "status", data_kv.key.via.str.size) == 0 &&
                            data_kv.val.type == MSGPACK_OBJECT_STR &&
                            strncmp(data_kv.val.via.str.ptr, "sucesso", data_kv.val.via.str.size) == 0) {
                            
                            // Login bem-sucedido
                            strncpy(client->current_user, username, sizeof(client->current_user)-1);
                            zmq_setsockopt(client->sub_socket, ZMQ_SUBSCRIBE, username, strlen(username));
                            printf("✅ Login REAL realizado como: %s\n", username);
                            msgpack_sbuffer_destroy(&sbuf);
                            return 1;
                        }
                    }
                }
            }
        }
    }
    
    printf("❌ Falha no login REAL\n");
    msgpack_sbuffer_destroy(&sbuf);
    return 0;
}

int bbs_client_list_users(BBSClient *client) {
    printf("\n👥 Buscando lista REAL de usuários...\n");
    
    // Prepara requisição
    msgpack_sbuffer sbuf;
    msgpack_packer pk;
    
    msgpack_sbuffer_init(&sbuf);
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);
    
    // {service: "users", data: {timestamp: "..."}}
    msgpack_pack_map(&pk, 2);
    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "service", 7);
    msgpack_pack_str(&pk, 5);
    msgpack_pack_str_body(&pk, "users", 5);
    
    msgpack_pack_str(&pk, 4);
    msgpack_pack_str_body(&pk, "data", 4);
    msgpack_pack_map(&pk, 1);
    
    msgpack_pack_str(&pk, 9);
    msgpack_pack_str_body(&pk, "timestamp", 9);
    char *timestamp = get_current_timestamp();
    msgpack_pack_str(&pk, strlen(timestamp));
    msgpack_pack_str_body(&pk, timestamp, strlen(timestamp));
    
    // Envia e recebe
    if (send_messagepack_request(client, "list_users", &sbuf) == 0) {
        msgpack_object response;
        if (receive_messagepack_response(client, &response) == 0) {
            char **users;
            int user_count;
            
            if (extract_string_array_from_response(response, "users", &users, &user_count)) {
                printf("👥 Usuários cadastrados (%d):\n", user_count);
                for (int i = 0; i < user_count; i++) {
                    printf("  - %s\n", users[i]);
                    free(users[i]);
                }
                free(users);
                msgpack_sbuffer_destroy(&sbuf);
                return 1;
            }
        }
    }
    
    printf("  ❌ Erro ao buscar usuários\n");
    msgpack_sbuffer_destroy(&sbuf);
    return 0;
}

int bbs_client_list_channels(BBSClient *client) {
    printf("\n📺 Buscando lista REAL de canais...\n");
    
    // Prepara requisição
    msgpack_sbuffer sbuf;
    msgpack_packer pk;
    
    msgpack_sbuffer_init(&sbuf);
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);
    
    // {service: "channels", data: {timestamp: "..."}}
    msgpack_pack_map(&pk, 2);
    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "service", 7);
    msgpack_pack_str(&pk, 8);
    msgpack_pack_str_body(&pk, "channels", 8);
    
    msgpack_pack_str(&pk, 4);
    msgpack_pack_str_body(&pk, "data", 4);
    msgpack_pack_map(&pk, 1);
    
    msgpack_pack_str(&pk, 9);
    msgpack_pack_str_body(&pk, "timestamp", 9);
    char *timestamp = get_current_timestamp();
    msgpack_pack_str(&pk, strlen(timestamp));
    msgpack_pack_str_body(&pk, timestamp, strlen(timestamp));
    
    // Envia e recebe
    if (send_messagepack_request(client, "list_channels", &sbuf) == 0) {
        msgpack_object response;
        if (receive_messagepack_response(client, &response) == 0) {
            char **channels;
            int channel_count;
            
            if (extract_string_array_from_response(response, "channels", &channels, &channel_count)) {
                printf("📺 Canais disponíveis (%d):\n", channel_count);
                for (int i = 0; i < channel_count; i++) {
                    printf("  %d. #%s\n", i + 1, channels[i]);
                    free(channels[i]);
                }
                free(channels);
                msgpack_sbuffer_destroy(&sbuf);
                return 1;
            }
        }
    }
    
    printf("  ❌ Erro ao buscar canais\n");
    msgpack_sbuffer_destroy(&sbuf);
    return 0;
}

int bbs_client_create_channel(BBSClient *client, const char *channel_name) {
    printf("\n➕ Criando canal REAL: #%s\n", channel_name);
    
    // Prepara requisição
    msgpack_sbuffer sbuf;
    msgpack_packer pk;
    
    msgpack_sbuffer_init(&sbuf);
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);
    
    // {service: "channel", data: {channel: "...", timestamp: "..."}}
    msgpack_pack_map(&pk, 2);
    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "service", 7);
    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "channel", 7);
    
    msgpack_pack_str(&pk, 4);
    msgpack_pack_str_body(&pk, "data", 4);
    msgpack_pack_map(&pk, 2);
    
    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "channel", 7);
    msgpack_pack_str(&pk, strlen(channel_name));
    msgpack_pack_str_body(&pk, channel_name, strlen(channel_name));
    
    msgpack_pack_str(&pk, 9);
    msgpack_pack_str_body(&pk, "timestamp", 9);
    char *timestamp = get_current_timestamp();
    msgpack_pack_str(&pk, strlen(timestamp));
    msgpack_pack_str_body(&pk, timestamp, strlen(timestamp));
    
    // Envia e recebe
    if (send_messagepack_request(client, "create_channel", &sbuf) == 0) {
        msgpack_object response;
        if (receive_messagepack_response(client, &response) == 0) {
            if (response.type == MSGPACK_OBJECT_MAP) {
                for (uint32_t i = 0; i < response.via.map.size; i++) {
                    msgpack_object_kv kv = response.via.map.ptr[i];
                    if (kv.key.type == MSGPACK_OBJECT_STR && 
                        strncmp(kv.key.via.str.ptr, "data", kv.key.via.str.size) == 0) {
                        
                        if (kv.val.type == MSGPACK_OBJECT_MAP) {
                            for (uint32_t j = 0; j < kv.val.via.map.size; j++) {
                                msgpack_object_kv data_kv = kv.val.via.map.ptr[j];
                                if (data_kv.key.type == MSGPACK_OBJECT_STR &&
                                    strncmp(data_kv.key.via.str.ptr, "status", data_kv.key.via.str.size) == 0 &&
                                    data_kv.val.type == MSGPACK_OBJECT_STR &&
                                    strncmp(data_kv.val.via.str.ptr, "sucesso", data_kv.val.via.str.size) == 0) {
                                    
                                    printf("✅ Canal '#%s' criado com sucesso!\n", channel_name);
                                    bbs_client_subscribe_channel(client, channel_name);
                                    msgpack_sbuffer_destroy(&sbuf);
                                    return 1;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    printf("❌ Erro ao criar canal\n");
    msgpack_sbuffer_destroy(&sbuf);
    return 0;
}

int bbs_client_subscribe_channel(BBSClient *client, const char *channel_name) {
    zmq_setsockopt(client->sub_socket, ZMQ_SUBSCRIBE, channel_name, strlen(channel_name));
    printf("📡 Inscrito no canal: #%s\n", channel_name);
    return 1;
}

int bbs_client_publish_message(BBSClient *client, const char *channel, const char *message) {
    printf("\n📢 Publicando mensagem REAL em #%s\n", channel);
    
    // Prepara requisição
    msgpack_sbuffer sbuf;
    msgpack_packer pk;
    
    msgpack_sbuffer_init(&sbuf);
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);
    
    // {service: "publish", data: {user: "...", channel: "...", message: "...", timestamp: "..."}}
    msgpack_pack_map(&pk, 2);
    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "service", 7);
    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "publish", 7);
    
    msgpack_pack_str(&pk, 4);
    msgpack_pack_str_body(&pk, "data", 4);
    msgpack_pack_map(&pk, 4);
    
    // user
    msgpack_pack_str(&pk, 4);
    msgpack_pack_str_body(&pk, "user", 4);
    msgpack_pack_str(&pk, strlen(client->current_user));
    msgpack_pack_str_body(&pk, client->current_user, strlen(client->current_user));
    
    // channel
    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "channel", 7);
    msgpack_pack_str(&pk, strlen(channel));
    msgpack_pack_str_body(&pk, channel, strlen(channel));
    
    // message
    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "message", 7);
    msgpack_pack_str(&pk, strlen(message));
    msgpack_pack_str_body(&pk, message, strlen(message));
    
    // timestamp
    msgpack_pack_str(&pk, 9);
    msgpack_pack_str_body(&pk, "timestamp", 9);
    char *timestamp = get_current_timestamp();
    msgpack_pack_str(&pk, strlen(timestamp));
    msgpack_pack_str_body(&pk, timestamp, strlen(timestamp));
    
    // Envia e recebe
    if (send_messagepack_request(client, "publish", &sbuf) == 0) {
        msgpack_object response;
        if (receive_messagepack_response(client, &response) == 0) {
            if (response.type == MSGPACK_OBJECT_MAP) {
                for (uint32_t i = 0; i < response.via.map.size; i++) {
                    msgpack_object_kv kv = response.via.map.ptr[i];
                    if (kv.key.type == MSGPACK_OBJECT_STR && 
                        strncmp(kv.key.via.str.ptr, "data", kv.key.via.str.size) == 0) {
                        
                        if (kv.val.type == MSGPACK_OBJECT_MAP) {
                            for (uint32_t j = 0; j < kv.val.via.map.size; j++) {
                                msgpack_object_kv data_kv = kv.val.via.map.ptr[j];
                                if (data_kv.key.type == MSGPACK_OBJECT_STR &&
                                    strncmp(data_kv.key.via.str.ptr, "status", data_kv.key.via.str.size) == 0 &&
                                    data_kv.val.type == MSGPACK_OBJECT_STR &&
                                    strncmp(data_kv.val.via.str.ptr, "OK", data_kv.val.via.str.size) == 0) {
                                    
                                    printf("✅ Mensagem publicada em #%s\n", channel);
                                    msgpack_sbuffer_destroy(&sbuf);
                                    return 1;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    printf("❌ Erro ao publicar mensagem\n");
    msgpack_sbuffer_destroy(&sbuf);
    return 0;
}

int bbs_client_send_direct_message(BBSClient *client, const char *target_user, const char *message) {
    printf("\n💌 Enviando mensagem direta REAL para %s\n", target_user);
    
    // Prepara requisição
    msgpack_sbuffer sbuf;
    msgpack_packer pk;
    
    msgpack_sbuffer_init(&sbuf);
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);
    
    // {service: "message", data: {src: "...", dst: "...", message: "...", timestamp: "..."}}
    msgpack_pack_map(&pk, 2);
    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "service", 7);
    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "message", 7);
    
    msgpack_pack_str(&pk, 4);
    msgpack_pack_str_body(&pk, "data", 4);
    msgpack_pack_map(&pk, 4);
    
    // src
    msgpack_pack_str(&pk, 3);
    msgpack_pack_str_body(&pk, "src", 3);
    msgpack_pack_str(&pk, strlen(client->current_user));
    msgpack_pack_str_body(&pk, client->current_user, strlen(client->current_user));
    
    // dst
    msgpack_pack_str(&pk, 3);
    msgpack_pack_str_body(&pk, "dst", 3);
    msgpack_pack_str(&pk, strlen(target_user));
    msgpack_pack_str_body(&pk, target_user, strlen(target_user));
    
    // message
    msgpack_pack_str(&pk, 7);
    msgpack_pack_str_body(&pk, "message", 7);
    msgpack_pack_str(&pk, strlen(message));
    msgpack_pack_str_body(&pk, message, strlen(message));
    
    // timestamp
    msgpack_pack_str(&pk, 9);
    msgpack_pack_str_body(&pk, "timestamp", 9);
    char *timestamp = get_current_timestamp();
    msgpack_pack_str(&pk, strlen(timestamp));
    msgpack_pack_str_body(&pk, timestamp, strlen(timestamp));
    
    // Envia e recebe
    if (send_messagepack_request(client, "direct_message", &sbuf) == 0) {
        msgpack_object response;
        if (receive_messagepack_response(client, &response) == 0) {
            if (response.type == MSGPACK_OBJECT_MAP) {
                for (uint32_t i = 0; i < response.via.map.size; i++) {
                    msgpack_object_kv kv = response.via.map.ptr[i];
                    if (kv.key.type == MSGPACK_OBJECT_STR && 
                        strncmp(kv.key.via.str.ptr, "data", kv.key.via.str.size) == 0) {
                        
                        if (kv.val.type == MSGPACK_OBJECT_MAP) {
                            for (uint32_t j = 0; j < kv.val.via.map.size; j++) {
                                msgpack_object_kv data_kv = kv.val.via.map.ptr[j];
                                if (data_kv.key.type == MSGPACK_OBJECT_STR &&
                                    strncmp(data_kv.key.via.str.ptr, "status", data_kv.key.via.str.size) == 0 &&
                                    data_kv.val.type == MSGPACK_OBJECT_STR &&
                                    strncmp(data_kv.val.via.str.ptr, "OK", data_kv.val.via.str.size) == 0) {
                                    
                                    printf("✅ Mensagem enviada para %s\n", target_user);
                                    msgpack_sbuffer_destroy(&sbuf);
                                    return 1;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    printf("❌ Erro ao enviar mensagem direta\n");
    msgpack_sbuffer_destroy(&sbuf);
    return 0;
}


void *listen_messages_thread(void *arg) {
    BBSClient *client = (BBSClient *)arg;

    if (!client || !client->sub_socket) {
        fprintf(stderr, "Erro: cliente ou socket de inscrição inválido.\n");
        return NULL;
    }

    while (client->listening) {
        char buffer[MAX_BUFFER];
        int size = zmq_recv(client->sub_socket, buffer, sizeof(buffer), 0);

        if (size <= 0) {
            if (errno == ETERM) break; // contexto encerrado
            continue;
        }

        // Inicializa msgpack unpacker
        msgpack_unpacked msg;
        msgpack_unpacked_init(&msg);

        bool success = msgpack_unpack_next(&msg, buffer, size, NULL);
        if (!success) {
            msgpack_unpacked_destroy(&msg);
            continue; // pula mensagens inválidas
        }

        msgpack_object root = msg.data;

        // Valida se é mapa e tem "service" e "data"
        if (root.type == MSGPACK_OBJECT_MAP) {
            msgpack_object_kv* kv = root.via.map.ptr;
            uint32_t map_size = root.via.map.size;

            msgpack_object service_obj = {0};
            msgpack_object data_obj = {0};

            for (uint32_t i = 0; i < map_size; i++) {
                if (kv[i].key.type == MSGPACK_OBJECT_STR) {
                    if (strncmp(kv[i].key.via.str.ptr, "service", kv[i].key.via.str.size) == 0)
                        service_obj = kv[i].val;
                    else if (strncmp(kv[i].key.via.str.ptr, "data", kv[i].key.via.str.size) == 0)
                        data_obj = kv[i].val;
                }
            }

            // Processa de acordo com o tipo de serviço
            if (service_obj.type == MSGPACK_OBJECT_STR) {
                if (strncmp(service_obj.via.str.ptr, "message", service_obj.via.str.size) == 0) {
                    process_direct_message(data_obj, client->current_user);
                } else if (strncmp(service_obj.via.str.ptr, "publish", service_obj.via.str.size) == 0) {
                    process_channel_message(data_obj, client->current_user);
                }
            }
        }

        msgpack_unpacked_destroy(&msg);
    }

    return NULL;
}



// Função para processar mensagens diretas
void process_direct_message(msgpack_object data_obj, const char *current_user) {
    msgpack_object_kv *kv;
    msgpack_object obj;

    // Acessa "src"
    kv = data_obj.via.map.ptr;
    for (int i = 0; i < data_obj.via.map.size; i++) {
        if (strncmp(kv[i].key.via.str.ptr, "src", kv[i].key.via.str.size) == 0) {
            obj = kv[i].val;
            printf("\n📨 Mensagem recebida de %.*s: ", (int)obj.via.str.size, obj.via.str.ptr);
        }
        if (strncmp(kv[i].key.via.str.ptr, "message", kv[i].key.via.str.size) == 0) {
            obj = kv[i].val;
            printf("%.*s\n", (int)obj.via.str.size, obj.via.str.ptr);
        }
    }

    printf("BBS %s > ", current_user);
    fflush(stdout);
}

// Função para processar mensagens de canal
void process_channel_message(msgpack_object data_obj, const char *current_user) {
    // exemplo para pegar "channel" e "message"
    for (size_t i = 0; i < data_obj.via.map.size; i++) {
        msgpack_object_kv kv = data_obj.via.map.ptr[i];
        if (strncmp(kv.key.via.str.ptr, "channel", kv.key.via.str.size) == 0) {
            printf("\nCanal: %.*s\n", (int)kv.val.via.str.size, kv.val.via.str.ptr);
        }
        if (strncmp(kv.key.via.str.ptr, "message", kv.key.via.str.size) == 0) {
            printf("%.*s\n", (int)kv.val.via.str.size, kv.val.via.str.ptr);
        }
    }
}


void bbs_client_start_listening(BBSClient *client) {
    keep_listening = 1;
    client->listening = 1;
    pthread_create(&client->listener_thread, NULL, listen_messages_thread, client);
    printf("🔊 Escutando mensagens em background...\n");
}

void bbs_client_stop_listening(BBSClient *client) {
    keep_listening = 0;
    client->listening = 0;
}

void bbs_client_show_welcome() {
    printf("\n");
    printf("==================================================\n");
    printf("          🚀 SISTEMA BBS/IRC - C LANG\n");
    printf("==================================================\n");
}

void bbs_client_show_menu(const char *username) {
    printf("\n--- BBS User: %s ---\n", username);
    printf("1. 📋 Listar usuários\n");
    printf("2. 📺 Listar canais\n");
    printf("3. ➕ Criar canal\n");
    printf("4. 📢 Publicar em canal\n");
    printf("5. 💌 Enviar mensagem direta\n");
    printf("6. 📡 Inscrever em canal\n");
    printf("7. 🚪 Sair\n");
}

void bbs_client_interactive_mode(BBSClient *client) {
    bbs_client_show_welcome();
    
    // Login
    while (strlen(client->current_user) == 0) {
        char username[50];
        printf("\nDigite seu nome de usuário: ");
        fflush(stdout);
        
        if (fgets(username, sizeof(username), stdin)) {
            trim_newline(username);
            if (strlen(username) > 0) {
                if (bbs_client_login(client, username)) {
                    break;
                }
            } else {
                printf("❌ Nome de usuário não pode estar vazio\n");
            }
        }
    }
    
    // Inscreve em canal geral
    bbs_client_subscribe_channel(client, "geral");
    printf("✅ Inscrito automaticamente no canal #geral\n");
    
    // Inicia escuta
    bbs_client_start_listening(client);
    
    // Menu principal
    char option[10];
    while (1) {
        printf("\n========================================\n");
        printf("BBS User: %s\n", client->current_user);
        printf("========================================\n");
        bbs_client_show_menu(client->current_user);
        printf("----------------------------------------\n");
        printf("Escolha uma opção: ");
        fflush(stdout);
        
        if (fgets(option, sizeof(option), stdin)) {
            trim_newline(option);
            
            switch (option[0]) {
                case '1':
                    bbs_client_list_users(client);
                    break;
                case '2':
                    bbs_client_list_channels(client);
                    break;
                case '3': {
                    char channel_name[50];
                    printf("Nome do novo canal: ");
                    fflush(stdout);
                    if (fgets(channel_name, sizeof(channel_name), stdin)) {
                        trim_newline(channel_name);
                        if (strlen(channel_name) > 0) {
                            bbs_client_create_channel(client, channel_name);
                        }
                    }
                    break;
                }
                case '4': {
                    char channel[50], message[256];
                    printf("Canal: ");
                    fflush(stdout);
                    if (fgets(channel, sizeof(channel), stdin)) {
                        trim_newline(channel);
                        printf("Mensagem: ");
                        fflush(stdout);
                        if (fgets(message, sizeof(message), stdin)) {
                            trim_newline(message);
                            if (strlen(channel) > 0 && strlen(message) > 0) {
                                bbs_client_publish_message(client, channel, message);
                            }
                        }
                    }
                    break;
                }
                case '5': {
                    char target[50], message[256];
                    printf("Usuário destino: ");
                    fflush(stdout);
                    if (fgets(target, sizeof(target), stdin)) {
                        trim_newline(target);
                        printf("Mensagem: ");
                        fflush(stdout);
                        if (fgets(message, sizeof(message), stdin)) {
                            trim_newline(message);
                            if (strlen(target) > 0 && strlen(message) > 0) {
                                bbs_client_send_direct_message(client, target, message);
                            }
                        }
                    }
                    break;
                }
                case '6': {
                    char channel[50];
                    printf("Nome do canal para se inscrever: ");
                    fflush(stdout);
                    if (fgets(channel, sizeof(channel), stdin)) {
                        trim_newline(channel);
                        if (strlen(channel) > 0) {
                            bbs_client_subscribe_channel(client, channel);
                        }
                    }
                    break;
                }
                case '7':
                    printf("\n👋 Saindo do sistema...\n");
                    bbs_client_stop_listening(client);
                    return;
                default:
                    printf("❌ Opção inválida. Digite um número de 1 a 7.\n");
            }
            
            sleep(1);
        }
    }
}

int main() {
    BBSClient client;
    
    printf("🎯 Cliente BBS em C - Iniciando...\n");
    
    // CORREÇÃO: Passar os parâmetros corretos
    if (bbs_client_init(&client, "servidor", "5555") != 0) {
        fprintf(stderr, "❌ Falha ao inicializar cliente\n");
        return 1;
    }
    
    bbs_client_interactive_mode(&client);
    bbs_client_cleanup(&client);
    
    return 0;
}