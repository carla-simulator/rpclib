#include "rpc/server.h"

#include <atomic>
#include <memory>
#include <stdexcept>
#include <stdint.h>
#include <thread>

#include "asio.hpp"
#include "format.h"

#include "rpc/detail/dev_utils.h"
#include "rpc/detail/log.h"
#include "rpc/detail/log.h"
#include "rpc/detail/server_session.h"
#include "rpc/detail/thread_group.h"
#include "rpc/this_session.h"

using namespace rpc::detail;
using RPCLIB_ASIO::ip::tcp;
using namespace RPCLIB_ASIO;

namespace rpc {

struct server::impl {
    impl(server *parent, std::string const &address, uint16_t port)
        : parent_(parent),
          io_(),
          acceptor_(io_,
                    tcp::endpoint(ip::address::from_string(address), port)),
          socket_(io_),
          suppress_exceptions_(false) {}

    impl(server *parent, uint16_t port)
        : parent_(parent),
          io_(),
          acceptor_(io_, tcp::endpoint(tcp::v4(), port)),
          socket_(io_),
          suppress_exceptions_(false) {}

    void start_accept() {
        acceptor_.async_accept(socket_, [this](std::error_code ec) {
            std::lock_guard<std::mutex> lock(_mutex);
            if (!ec) {
                LOG_INFO("Accepted connection.");
                socket_.set_option(RPCLIB_ASIO::ip::tcp::no_delay(true));
                auto s = std::make_shared<server_session>(
                    parent_, &io_, std::move(socket_), parent_->disp_,
                    suppress_exceptions_);
                s->start();
                sessions_.push_back(s);
                if (callback_on_connection_) {
                    this_session().set_id(reinterpret_cast<session_id_t>(s.get()));
                    callback_on_connection_(s);
                }
            } else {
                LOG_ERROR("Error while accepting connection: {}", ec);
            }
            start_accept();
            // TODO: allow graceful exit [sztomi 2016-01-13]
        });
    }

    void close_sessions() {
        std::lock_guard<std::mutex> lock(_mutex);
        for (auto &session : sessions_) {
            session->close();
        }
        sessions_.clear();
    }

    void close_session(std::shared_ptr<detail::server_session> const &s) {
        std::lock_guard<std::mutex> lock(_mutex);
        auto it = std::find(begin(sessions_), end(sessions_), s);
        if (it != end(sessions_)) {
            if (callback_on_disconnection_) {
                this_session().set_id(reinterpret_cast<session_id_t>(s.get()));
                callback_on_disconnection_(*it);
            }
            sessions_.erase(it);
        }
    }

    void stop() {
        io_.stop();
        loop_workers_.join_all();
    }

    server *parent_;
    io_service io_;
    ip::tcp::acceptor acceptor_;
    ip::tcp::socket socket_;
    rpc::detail::thread_group loop_workers_;
    std::vector<std::shared_ptr<server_session>> sessions_;
    std::atomic_bool suppress_exceptions_;
    callback_type callback_on_connection_;
    callback_type callback_on_disconnection_;
    std::mutex _mutex;
    RPCLIB_CREATE_LOG_CHANNEL(server)
};

RPCLIB_CREATE_LOG_CHANNEL(server)

server::server(uint16_t port)
    : pimpl(new server::impl(this, port)), disp_(std::make_shared<dispatcher>()) {
    LOG_INFO("Created server on localhost:{}", port);
    pimpl->start_accept();
}

server::server(server&& other) noexcept {
    *this = std::move(other);
}

server::server(std::string const &address, uint16_t port)
    : pimpl(new server::impl(this, address, port)),
    disp_(std::make_shared<dispatcher>()) {
    LOG_INFO("Created server on address {}:{}", address, port);
    pimpl->start_accept();
}

server::~server() {
    if (pimpl) {
        pimpl->stop();
    }
}

server& server::operator=(server &&other) {
    pimpl = std::move(other.pimpl);
    other.pimpl = nullptr;
    disp_ = std::move(other.disp_);
    other.disp_ = nullptr;
    return *this;
}

void server::suppress_exceptions(bool suppress) {
    pimpl->suppress_exceptions_ = suppress;
}

void server::run() { pimpl->io_.run(); }

void server::async_run(std::size_t worker_threads) {
    pimpl->loop_workers_.create_threads(worker_threads, [this]() {
        name_thread("server");
        LOG_INFO("Starting");
        pimpl->io_.run();
        LOG_INFO("Exiting");
    });
}

void server::stop() { pimpl->stop(); }

void server::close_sessions() { pimpl->close_sessions(); }

void server::close_session(std::shared_ptr<detail::server_session> const &s) {
    pimpl->close_session(s);
}

void server::set_on_connection(callback_type obj) {
    pimpl->callback_on_connection_ = obj;
}

void server::set_on_disconnection(callback_type obj) {
    pimpl->callback_on_disconnection_ = obj;
}

} /* rpc */
