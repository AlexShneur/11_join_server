#pragma once
#include <cstdlib>
#include <iostream>
#include <memory>
#include <utility>
#include <boost/asio.hpp>

#include "database.h"

using boost::asio::ip::tcp;

class session
    : public std::enable_shared_from_this<session>
{
public:
    session(tcp::socket socket, std::shared_ptr<Database> _db)
        : socket_(std::move(socket)), db(_db)
    {

    }

    void start()
    {
        do_read();
    }

private:
    void do_read()
    {
        auto self(shared_from_this());
        socket_.async_read_some(boost::asio::buffer(data_, max_length),
            [this, self](boost::system::error_code ec, std::size_t bytes_received)
            {
                if (!ec)
                {
                    auto message = std::string{ data_, bytes_received };
                    std::string error="";
                    std::string result_str = "";
                    if (db->execute_query(message, error, result_str)== SQLITE_DONE)
                    {
                        result_str += "OK\n";
                    }
                    else
                    {
                        result_str = "ERR "+ error+"\n";
                    }
                    result_str.copy(data_, result_str.size());
                    do_write(result_str.size());
                }
            });
    }


    void do_write(std::size_t length)
    {
        auto self(shared_from_this());
        boost::asio::async_write(socket_, boost::asio::buffer(data_, length),
            [this, self](boost::system::error_code ec, std::size_t bytes_transferred)
            {
                if (!ec)
                {
                    do_read();
                }
            });
    }

    tcp::socket socket_;
    enum { max_length = 131072 };
    char data_[max_length];

    std::shared_ptr<Database> db;
};

class server
{
public:
    server(boost::asio::io_context& io_context, short port)
        : acceptor_(io_context, tcp::endpoint(tcp::v4(), port))
    {
        db = std::make_shared<Database>();
        do_accept();
    }

private:
    void do_accept()
    {
        acceptor_.async_accept(
            [this](boost::system::error_code ec, tcp::socket socket)
            {
                if (!ec)
                {
                    std::make_shared<session>(std::move(socket), db)->start();
                }

                do_accept();
            });
    }

    tcp::acceptor acceptor_;
    std::shared_ptr<Database> db;
};
