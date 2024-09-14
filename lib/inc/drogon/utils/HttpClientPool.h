/**
 *
 *  @file CoroHttpClientPool.h
 *  @author fantasy-peak
 *
 *  Copyright 2021, Martin Chang.  All rights reserved.
 *  https://github.com/an-tao/drogon
 *  Use of this source code is governed by a MIT license
 *  that can be found in the License file.
 *
 *  Drogon
 *
 */
#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <trantor/utils/Logger.h>
#include <trantor/net/EventLoopThreadPool.h>
#ifdef __cpp_impl_coroutine
#include <drogon/utils/coroutine.h>
#endif
#include <drogon/HttpTypes.h>
#include <drogon/drogon.h>

namespace drogon
{

struct HttpClientPoolConfig
{
    std::string hostString;
    bool useOldTLS{false};
    bool validateCert{false};
    std::size_t minSize_{10};
    std::size_t maxSize_{100};
    std::function<void(HttpClientPtr &)> setCallback;
    std::size_t numOfThreads{std::thread::hardware_concurrency()};
    std::optional<std::size_t> keepaliveRequests;
    std::optional<std::chrono::seconds> keepaliveTimeout;
    std::optional<std::chrono::seconds> keepaliveTime;
    std::vector<drogon::ReqResult> retryError;
    std::size_t retry{2};
    std::chrono::milliseconds retryInterval{100};
    std::chrono::seconds checkInterval{300};
};

class HttpPoolException : public std::exception
{
  public:
    HttpPoolException(ReqResult res, std::string msg)
        : resultCode_(res), message_(std::move(msg))
    {
    }

    const char *what() const noexcept override
    {
        return message_.data();
    }

    ReqResult code() const
    {
        return resultCode_;
    }

  private:
    ReqResult resultCode_;
    std::string message_;
};

#ifdef __cpp_impl_coroutine

class CoroHttpClientPool final
{
  public:
    CoroHttpClientPool(
        HttpClientPoolConfig cfg,
        std::shared_ptr<trantor::EventLoopThreadPool> pool = nullptr,
        std::shared_ptr<trantor::EventLoopThreadPool> dispatchPool = nullptr)
        : cfg_(std::move(cfg)),
          loopPool_(std::move(pool)),
          dispatchPool_(std::move(dispatchPool))
    {
        if (!loopPool_)
        {
            loopPool_ = std::make_unique<trantor::EventLoopThreadPool>(
                cfg_.numOfThreads);
            loopPool_->start();
        }
        if (!dispatchPool_)
        {
            dispatchPool_ = std::make_unique<trantor::EventLoopThreadPool>(1);
            dispatchPool_->start();
        }
        loopPtr_ = dispatchPool_->getNextLoop();

        if (cfg_.keepaliveTimeout.has_value() || cfg_.keepaliveTime.has_value())
        {
            timerId_ = loopPtr_->runEvery(cfg_.checkInterval, [this] {
                LOG_DEBUG << "httpClients_ size:" << httpClients_.size();
                if (httpClients_.size() <= cfg_.minSize_)
                    return;
                std::queue<std::shared_ptr<Connection>> busyClients;
                while (!httpClients_.empty())
                {
                    auto connPtr = std::move(httpClients_.front());
                    httpClients_.pop();
                    if (connPtr->check(cfg_))
                        busyClients.emplace(std::move(connPtr));
                }
                if (busyClients.size() >= cfg_.minSize_)
                {
                    httpClients_ = std::move(busyClients);
                }
                else
                {
                    if (!busyClients.empty())
                        httpClients_ = std::move(busyClients);
                    auto count = cfg_.minSize_ - httpClients_.size();
                    for (int i = 0; i < count; i++)
                    {
                        httpClients_.emplace(std::make_shared<Connection>(
                            createHttpClientFunc_));
                    }
                }
            });
        }

        createHttpClientFunc_ = [this]() mutable {
            auto client = HttpClient::newHttpClient(cfg_.hostString,
                                                    loopPool_->getNextLoop(),
                                                    cfg_.useOldTLS,
                                                    cfg_.validateCert);
            if (cfg_.setCallback)
                cfg_.setCallback(client);
            return client;
        };

        for (std::size_t i = 0; i < cfg_.minSize_; i++)
        {
            httpClients_.emplace(
                std::make_shared<Connection>(createHttpClientFunc_));
        }
        httpClientSize_ = cfg_.minSize_;
    }

    ~CoroHttpClientPool()
    {
        std::promise<void> done;
        loopPtr_->runInLoop([this, &done] {
            std::queue<std::shared_ptr<Connection>> tmp;
            httpClients_.swap(tmp);
            if (timerId_)
                loopPtr_->invalidateTimer(timerId_.value());
            done.set_value();
        });
        done.get_future().wait();
        [](auto &&...args) {
            auto func = [](auto &poolPtr) {
                if (poolPtr == nullptr)
                    return;
                for (auto &ptr : poolPtr->getLoops())
                    ptr->runInLoop([=] { ptr->quit(); });
                poolPtr->wait();
                poolPtr.reset();
            };
            (func(args), ...);
        }(dispatchPool_, loopPool_);
    }

    CoroHttpClientPool(const CoroHttpClientPool &) = delete;
    CoroHttpClientPool &operator=(const CoroHttpClientPool &) = delete;
    CoroHttpClientPool(CoroHttpClientPool &&) = delete;
    CoroHttpClientPool &operator=(CoroHttpClientPool &&) = delete;

    struct Connection
    {
        Connection(const std::function<HttpClientPtr()> &createHttpClientFunc)
        {
            clientPtr_ = createHttpClientFunc();
            auto now = std::chrono::system_clock::now();
            timePoint_ = now;
            startTimePoint_ = now;
        }

        bool checkConn(const HttpClientPoolConfig &cfg)
        {
            auto now = std::chrono::system_clock::now();
            auto idleDut = now - timePoint_;
            auto dut = now - startTimePoint_;
            if ((cfg.keepaliveRequests.has_value() &&
                 counter_ >= cfg.keepaliveRequests.value()) ||
                (cfg.keepaliveTimeout.has_value() &&
                 idleDut >= cfg.keepaliveTimeout.value()) ||
                (cfg.keepaliveTime.has_value() &&
                 dut >= cfg.keepaliveTime.value()))
            {
                return false;
            }
            return true;
        }

        bool check(const HttpClientPoolConfig &cfg)
        {
            auto now = std::chrono::system_clock::now();
            auto idleDut = now - timePoint_;
            auto dut = now - startTimePoint_;
            if ((cfg.keepaliveTimeout.has_value() &&
                 idleDut >= cfg.keepaliveTimeout.value()) ||
                (cfg.keepaliveTimeout.has_value() &&
                 dut >= cfg.keepaliveTime.value()))
            {
                return false;
            }
            return true;
        }

        void sendRequest(
            const HttpRequestPtr &req,
            std::function<void(ReqResult, const HttpResponsePtr &)> cb,
            double timeout = 0)
        {
            counter_++;
            clientPtr_->sendRequest(req, std::move(cb), timeout);
        }

        HttpClientPtr clientPtr_;
        std::chrono::time_point<std::chrono::system_clock> timePoint_;
        std::chrono::time_point<std::chrono::system_clock> startTimePoint_;
        std::size_t counter_{0};
    };

    struct HttpRespAwaiter
        : public CallbackAwaiter<std::tuple<ReqResult, HttpResponsePtr>>
    {
        HttpRespAwaiter(std::shared_ptr<Connection> conn,
                        HttpRequestPtr req,
                        double timeout)
            : connectionPtr_(std::move(conn)),
              req_(std::move(req)),
              timeout_(timeout)
        {
        }

        void await_suspend(std::coroutine_handle<> handle)
        {
            connectionPtr_->sendRequest(
                req_,
                [handle, this](ReqResult result,
                               const HttpResponsePtr &resp) mutable {
                    setValue(std::make_tuple(result, resp));
                    handle.resume();
                },
                timeout_);
        }

      private:
        std::shared_ptr<Connection> connectionPtr_;
        drogon::HttpRequestPtr req_;
        double timeout_;
    };

    struct Awaiter : public drogon::CallbackAwaiter<
                         std::optional<std::shared_ptr<Connection>>>
    {
        Awaiter(CoroHttpClientPool *client,
                trantor::EventLoop *loop,
                double timeout)
            : pool_(client), loop_(loop), timeout_(timeout)
        {
        }

        void await_suspend(std::coroutine_handle<> handle)
        {
            loop_->runInLoop([this, handle] {
                pool_->getConnection(
                    [this, handle](std::optional<std::shared_ptr<Connection>>
                                       opt) mutable {
                        setValue(std::move(opt));
                        handle.resume();
                    },
                    timeout_);
            });
        }

      private:
        CoroHttpClientPool *pool_;
        trantor::EventLoop *loop_;
        double timeout_;
    };

    template <typename F>
    struct ScopeExit
    {
        ScopeExit(F &&f) : f_(std::forward<F>(f))
        {
        }

        ~ScopeExit()
        {
            f_();
        }

        F f_;
    };

    template <typename F>
    ScopeExit<F> makeScopeExit(F &&f)
    {
        return ScopeExit<F>(std::forward<F>(f));
    };

    drogon::Task<drogon::HttpResponsePtr> sendRequestCoro(
        drogon::HttpRequestPtr req,
        double timeout)
    {
        auto connOpt = co_await Awaiter{this, loopPtr_, timeout};
        if (!connOpt.has_value())
        {
            throw HttpPoolException(ReqResult::Timeout,
                                    "get HttpClient timeout");
        }
        auto &connPtr = connOpt.value();

        auto backConn = makeScopeExit([connPtr, this]() mutable {
            loopPtr_->runInLoop([this, connPtr = std::move(connPtr)]() mutable {
                while (!coroutines_.empty())
                {
                    auto coro = std::move(coroutines_.front());
                    coroutines_.pop();
                    auto &[id, done, setValue] = coro;
                    if (*done)
                    {
                        continue;
                    }
                    *done = true;
                    if (id)
                        loopPtr_->invalidateTimer(id.value());
                    if (!connPtr->checkConn(cfg_))
                    {
                        LOG_DEBUG << "create new connPtr";
                        connPtr =
                            std::make_shared<Connection>(createHttpClientFunc_);
                    }
                    setValue(std::make_optional(std::move(connPtr)));
                    return;
                }
                httpClients_.emplace(std::move(connPtr));
            });
        });

        auto loopPtr = trantor::EventLoop::getEventLoopOfCurrentThread();

        std::size_t retry = 0;
        for (;;)
        {
            loopPtr_->assertInLoopThread();
            auto [result, resp] =
                co_await HttpRespAwaiter{connPtr, req, timeout};
            if (result == drogon::ReqResult::Ok)
                co_return resp;
            auto it = std::find_if(cfg_.retryError.begin(),
                                   cfg_.retryError.end(),
                                   [&](auto error) { return error = result; });
            if ((it != cfg_.retryError.end()) && (retry < cfg_.retry))
            {
                retry++;
                LOG_DEBUG << drogon::to_string_view(result) << " retry...";
                auto dut = cfg_.retryInterval * retry;
                co_await drogon::sleepCoro(loopPtr, dut);
                continue;
            }
            else
            {
                auto error = utils::formattedString(
                    "%s(%s)",
                    drogon::to_string_view(result).data(),
                    cfg_.hostString.data());
                throw HttpPoolException(result, std::move(error));
            }
        }
    }

    void sendRequest(const HttpRequestPtr &req,
                     HttpReqCallback &&callback,
                     double timeout = 0)
    {
        [](auto self,
           const auto &req,
           auto &&callback,
           double timeout) -> AsyncTask {
            try
            {
                auto resp = co_await self->sendRequestCoro(req, timeout);
                callback(ReqResult::Ok, resp);
            }
            catch (const HttpPoolException &e)
            {
                callback(e.code(), nullptr);
            }
            co_return;
        }(this, req, std::move(callback), timeout);
    }

    auto &getEventLoopThreadPool()
    {
        return loopPool_;
    }

    auto getEventLoop()
    {
        return loopPtr_;
    }

  private:
    void getConnection(
        std::function<void(std::optional<std::shared_ptr<Connection>>)>
            setValue,
        double timeout)
    {
        if (httpClients_.empty())
        {
            if (httpClientSize_ < cfg_.maxSize_)
            {
                httpClientSize_++;
                LOG_DEBUG << "create new HttpClient:" << httpClientSize_;
                setValue(std::make_optional(
                    std::make_shared<Connection>(createHttpClientFunc_)));
                return;
            }
            auto done = std::make_shared<bool>(false);
            std::optional<trantor::TimerId> timerId;
            if (timeout > 0)
            {
                timerId = loopPtr_->runAfter(timeout, [this, setValue, done] {
                    if (*done)
                        return;
                    *done = true;
                    setValue(std::nullopt);
                });
            }
            coroutines_.emplace(Coroutine{
                .id = timerId,
                .done = done,
                .setValue = std::move(setValue),
            });
            LOG_DEBUG << "push coro to coroutines_:" << coroutines_.size();
        }
        else
        {
            auto connPtr = std::move(httpClients_.front());
            httpClients_.pop();
            if (!connPtr->checkConn(cfg_))
            {
                LOG_DEBUG << "create new connPtr";
                connPtr = std::make_shared<Connection>(createHttpClientFunc_);
            }
            LOG_DEBUG << "httpClients_ size:" << httpClients_.size();
            setValue(std::make_optional(std::move(connPtr)));
        }
    }

    HttpClientPoolConfig cfg_;
    std::shared_ptr<trantor::EventLoopThreadPool> loopPool_;
    std::shared_ptr<trantor::EventLoopThreadPool> dispatchPool_;
    trantor::EventLoop *loopPtr_;
    std::function<HttpClientPtr()> createHttpClientFunc_;

    struct Coroutine
    {
        std::optional<trantor::TimerId> id;
        std::shared_ptr<bool> done;
        std::function<void(std::optional<std::shared_ptr<Connection>>)>
            setValue;
    };

    std::queue<std::shared_ptr<Connection>> httpClients_;
    std::queue<Coroutine> coroutines_;
    std::size_t httpClientSize_;
    std::optional<trantor::TimerId> timerId_;
};

#endif

}  // namespace drogon
