#pragma once
#include "sqlite3.h"
#include <string>

class Database
{
public:

	Database()
	{
		open_db();
        create_table("A");
        create_table("B");
	}

    ~Database()
    {
        close_db();
    }

    int execute_query(const std::string& message, std::string& error, std::string& results)
    {
        auto query = parse(message);
        return execute_query_with_results(query, error, results);
    }

private:
	int open_db()
	{
		const char* db_name = "join_server_db.sqlite";
        int rc = sqlite3_open(db_name, &handle);
		if (rc)
		{
			sqlite3_close(handle);
		}
		return rc;
	}

    void close_db()
    {
        sqlite3_close(handle);
    }

    int create_table(const std::string& name)
    {
        std::string query = "CREATE TABLE "+name+"("  \
            "ID INT  PRIMARY KEY    NOT NULL," \
            "NAME           TEXT    NOT NULL);";
        std::string error = "";
        return execute_query(query, error);
    }

    int execute_query(const std::string& query, std::string& error)
    {
        char* errmsg;
        int rc = sqlite3_exec(handle, query.c_str(), nullptr, 0, &errmsg);
        if (rc != SQLITE_OK)
        {
            error = errmsg;
            sqlite3_free(errmsg);
        }
        return rc;
    }

    int execute_query_with_results(const std::string& query, std::string& error, std::string& results)
    {
        const char* sql = query.c_str();
        sqlite3_stmt* stmt;
        int rc = sqlite3_prepare_v2(handle, sql, -1, &stmt, NULL);
        if (rc != SQLITE_OK)
        {
            error = sqlite3_errmsg(handle);
            return rc;
        }
        while ((rc = sqlite3_step(stmt)) == SQLITE_ROW)
        {
            int id = sqlite3_column_int(stmt, 0);
            std::string name1 = "", name2 = "";
            auto res1 = sqlite3_column_text(stmt, 1);
            if (res1 != nullptr)
                name1 = reinterpret_cast<char const*>(res1);
            auto res2 = sqlite3_column_text(stmt, 2);
            if (res2 != nullptr)
                name2 = reinterpret_cast<char const*>(res2);
            std::string result = std::to_string(id) + "," + std::move(name1) + "," + std::move(name2);
            results+=std::move(result)+"\n";
        }
        if (rc != SQLITE_DONE)
        {
            error = sqlite3_errmsg(handle);

        }
        sqlite3_finalize(stmt);
        return rc;
    }

    std::string parse(std::string s)
    {
        std::string delimiter = " ";
        size_t pos = 0;
        std::string query;
        std::string table_name = "";
        while ((pos = s.find(delimiter)) != std::string::npos)
        {
            std::string token = s.substr(0, pos);
            if (token == "INSERT")
            {
                query += "INSERT INTO ";
            }
            else if (token == "TRUNCATE")
            {
                query += "DELETE FROM ";
            }
            else
            {
                if (table_name.empty())
                {
                    table_name = token;
                    query += table_name + " (ID, NAME)" \
                        " VALUES (";
                }
                else
                    query += token + ", ";

            }
            s.erase(0, pos + delimiter.length());
        }
        if (query.find("INSERT") != std::string::npos)
        {
            std::string str = "\'";
            query.append(str + s + str + ")");
        }
        else
        {
            if (s == "INTERSECTION")
            {
                query = "SELECT a.id AS id, a.name AS A, b.name AS B " \
                    "FROM A AS a, B AS b " \
                    "WHERE a.id=b.id";
            }
            else if (s == "SYMMETRIC_DIFFERENCE")
            {
                query = "SELECT id, name as A, NULL AS B FROM A " \
                    "WHERE id NOT IN " \
                    "(SELECT id FROM B) UNION " \
                    "SELECT id, name as B, NULL AS A FROM B " \
                    "WHERE id NOT IN " \
                    "(SELECT id FROM A)";
            }
        }
        return query;
    }

    sqlite3* handle = nullptr;
};