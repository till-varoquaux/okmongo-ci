#include "mongo.h"
#include "bson_dumper.h"
#include <iostream>

extern "C" {
#include <stdio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
}

//=============================================================================

// Trivial blocking IO interface for the network

static void send(int sockfd, const okmongo::BsonWriter &w) {
    const char *data = w.data();
    int32_t len = w.len();
    while (len > 0) {
        ssize_t res = write(sockfd, data, static_cast<size_t>(len));
        if (res == -1) {
            perror("Failed to write.");
            exit(1);
        }
        if (res == 0) {
            std::cerr << "Done before we expected" << std::endl;
            exit(1);
        }
        len -= static_cast<int32_t>(res);
        data += res;
    }
}

static int connect() {
    constexpr uint16_t port = 27017;
    constexpr const char *server = "127.0.0.1";

    struct sockaddr_in servaddr;
    bzero(&servaddr, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = inet_addr(server);
    servaddr.sin_port = htons(port);

    int sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (connect(sockfd, reinterpret_cast<struct sockaddr *>(&servaddr),
                sizeof(servaddr)) == -1) {
        perror("Failed to connect.");
        exit(1);
    }

    return sockfd;
}

template <typename T>
void receive(int sockfd, T *r) {
    char scratch[128];

    while (!r->Done()) {
        ssize_t res = read(sockfd, scratch, 128);
        if (res == -1) {
            perror("Failed to write.");
            exit(1);
        }
        if (res == 0) {
            std::cerr << "Done before we expected" << std::endl;
            exit(1);
        }
        r->Consume(scratch, static_cast<int32_t>(res));
    }
}

inline void cmd_receive(int sockfd) {
    okmongo::OpResponseParser r;
    receive(sockfd, &r);
    std::cout << r.Header().response_to << ":";
    if (!r.Result().errors.empty()) {
        std::cout << "errors: " << std::endl;
        for (auto &e : r.Result().errors) {
            std::cout << "   " << e.msg << std::endl;
        }
    } else {
        std::cout << "ok" << std::endl;
    }
}

//==============================================================================

static void AddIdx(okmongo::BsonWriter *w, int32_t requestid) {
    AppendCommandHeader(w, requestid, "mydb");

    w->Document();
    {
        w->Element("createIndexes", "users");

        w->PushArray("indexes");
        {
            w->PushDocument(0);
            {
                w->PushDocument("key");
                { w->Element("name", 1); }
                w->Pop();
                w->Element("unique", true);
                w->Element("name", "name_idx");
            }
            w->Pop();
        }
        w->Pop();
    }
    w->Pop();

    w->FlushLen();
}

class ResponseDumper
        : public okmongo::BsonDumper<okmongo::ResponseReader<ResponseDumper>> {
};

struct UserQuery {
    std::string name;
};

struct UserInfo {
    std::string name;
    int32_t counter;
};

struct LongUserInfo {
    std::string first_name;
    std::string last_name;
    int32_t counter;
};

struct All {};

struct IncCounter {
    int quantity;
};

namespace okmongo {
template <>
bool BsonWriteFields<All>(BsonWriter *, const All &) {
    return true;
}

template <>
bool BsonWriteFields<UserInfo>(BsonWriter *w, const UserInfo &inf) {
    w->Element("name", inf.name);
    w->Element("counter", inf.counter);
    return true;
}

template <>
bool BsonWriteFields<UserQuery>(BsonWriter *w, const UserQuery &inf) {
    w->PushArray("$or");
    int32_t key = 0;

    w->PushDocument(++key);
    w->Element("name", inf.name);
    w->Pop();

    w->PushDocument(++key);
    w->Element("first_name", inf.name);
    w->Pop();

    w->Pop();
    return true;
}

template <>
bool BsonWriteFields<LongUserInfo>(BsonWriter *w, const LongUserInfo &inf) {
    w->Element("first_name", inf.first_name);
    w->Element("last_name", inf.last_name);
    w->Element("counter", inf.counter);
    return true;
}

template <>
bool BsonWriteFields<IncCounter>(BsonWriter *w, const IncCounter &cnt) {
    w->PushDocument("$inc");
    w->Element("counter", static_cast<int32_t>(cnt.quantity));
    w->Pop();
    return true;
}
}  // namespace okmongo

int main() {
    int sockfd = connect();

    int request_id = 0;

    {
        okmongo::BsonWriter w;
        FillIsMasterOp(&w, ++request_id);
        send(sockfd, w);

        // Reading it back in...
        ResponseDumper r;
        receive(sockfd, &r);
    }

    {
        okmongo::BsonWriter w;
        AddIdx(&w, ++request_id);
        send(sockfd, w);

        ResponseDumper r;
        receive(sockfd, &r);
    }

    {
        okmongo::BsonWriter w;
        FillInsertOp(&w, ++request_id, "mydb", "users", UserInfo{"mike", 0},
                     LongUserInfo{"till", "varoquaux", 0}, UserInfo{"mike", 2});
        send(sockfd, w);
        cmd_receive(sockfd);
    }

    {
        okmongo::BsonWriter w;
        FillUpdateOp(&w, ++request_id, "mydb", "users", All{}, IncCounter{5});
        send(sockfd, w);
        cmd_receive(sockfd);
    }

    {
        okmongo::BsonWriter w;
        FillUpdateOp(&w, ++request_id, "mydb", "users", UserQuery{"till"},
                     IncCounter{-2});
        send(sockfd, w);
        cmd_receive(sockfd);
    }

    {
        okmongo::BsonWriter w;
        FillQueryOp(&w, ++request_id, "mydb", "users", All{});
        send(sockfd, w);

        // Reading it back in...
        ResponseDumper r;
        receive(sockfd, &r);
    }

    // {
    //     // Delete everything from the collection...
    //     okmongo::BsonWriter w;
    //     Delete(&w, ++request_id, "mydb", "users", All{});
    //     send(sockfd, w);

    //     cmd_receive(sockfd);
    // }
}
