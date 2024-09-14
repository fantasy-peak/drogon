#include <drogon/drogon.h>

#include <chrono>
#include <future>
#include <iostream>
#include <memory>
#include <thread>
#include "drogon/utils/coroutine.h"
#include "trantor/utils/Logger.h"

#ifdef __linux__
#include <sys/socket.h>
#include <netinet/tcp.h>
#endif
#include </root/github/drogon/lib/inc/drogon/utils/HttpClientPool.h>
using namespace drogon;

int nth_resp = 0;

int main()
{
    auto func = [](int fd) {
        std::cout << "setSockOptCallback:" << fd << std::endl;
#ifdef __linux__
        int optval = 10;
        ::setsockopt(fd,
                     SOL_TCP,
                     TCP_KEEPCNT,
                     &optval,
                     static_cast<socklen_t>(sizeof optval));
        ::setsockopt(fd,
                     SOL_TCP,
                     TCP_KEEPIDLE,
                     &optval,
                     static_cast<socklen_t>(sizeof optval));
        ::setsockopt(fd,
                     SOL_TCP,
                     TCP_KEEPINTVL,
                     &optval,
                     static_cast<socklen_t>(sizeof optval));
#endif
    };
    trantor::Logger::setLogLevel(trantor::Logger::kTrace);
#ifdef __cpp_impl_coroutine
    HttpClientPoolConfig cfg{
        .hostString = "http://www.baidu.com",
        .useOldTLS = false,
        .validateCert = false,
        .minSize_{2},
        .maxSize_{100},
        .setCallback =
            [func](auto &client) {
                LOG_INFO << "setCallback";
                client->setSockOptCallback(func);
            },
        .numOfThreads = 4,
        .keepaliveRequests = 1,
        .keepaliveTimeout = std::chrono::seconds(30),  // Idle time
        .keepaliveTime = std::chrono::seconds(120),    // Maximum lifecycle
        .retryError = {drogon::ReqResult::Timeout},
        .retry = 2,
        .retryInterval = std::chrono::milliseconds(100),
        .checkInterval = std::chrono::seconds(10),
    };
    auto pool = std::make_unique<CoroHttpClientPool>(cfg);
    auto req = HttpRequest::newHttpRequest();
    req->setMethod(drogon::Get);
    req->setPath("/s");
    req->setParameter("wd", "wx");
    req->setParameter("oq", "wx");
    for (int i = 0; i < 5; i++)
    {
        [&](int i) -> drogon::AsyncTask {
            LOG_INFO << "send:" << i;
            try
            {
                auto resp = co_await pool->sendRequestCoro(req, 10);
                LOG_INFO << resp->getStatusCode();
            }
            catch (const std::exception &e)
            {
                LOG_ERROR << e.what();
            }
            co_return;
        }(i);
    }
    pool->sendRequest(
        req,
        [](ReqResult result, const HttpResponsePtr &response) {
            if (result != ReqResult::Ok)
            {
                LOG_ERROR << "error while sending request to server! result: "
                          << result;
                return;
            }
            LOG_INFO << "callback:" << response->getStatusCode();
        },
        10);
    std::this_thread::sleep_for(std::chrono::seconds(200));
#else
    {
        auto client = HttpClient::newHttpClient("http://www.baidu.com");
        client->setSockOptCallback(func);

        auto req = HttpRequest::newHttpRequest();
        req->setMethod(drogon::Get);
        req->setPath("/s");
        req->setParameter("wd", "wx");
        req->setParameter("oq", "wx");

        for (int i = 0; i < 10; ++i)
        {
            client->sendRequest(
                req, [](ReqResult result, const HttpResponsePtr &response) {
                    if (result != ReqResult::Ok)
                    {
                        std::cout
                            << "error while sending request to server! result: "
                            << result << std::endl;
                        return;
                    }

                    std::cout << "receive response!" << std::endl;
                    // auto headers=response.
                    ++nth_resp;
                    std::cout << response->getBody() << std::endl;
                    auto cookies = response->cookies();
                    for (auto const &cookie : cookies)
                    {
                        std::cout << cookie.first << "="
                                  << cookie.second.value()
                                  << ":domain=" << cookie.second.domain()
                                  << std::endl;
                    }
                    std::cout << "count=" << nth_resp << std::endl;
                });
        }
        std::cout << "requestsBufferSize:" << client->requestsBufferSize()
                  << std::endl;
    }

    app().run();
#endif
}
