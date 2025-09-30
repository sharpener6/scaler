#pragma once

// System
#ifdef __linux__
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#endif  // __linux__

#ifdef __APPLE__
#include <sys/event.h>
#endif  // __APPLE__

#ifdef _WIN32
#include <winsock2.h>
#include <windows.h>
#else
#include <sys/socket.h>
#include <unistd.h>
#endif  // _WIN32

// C
#include <cerrno>

// C++
#include <expected>
#include <optional>

// First-party
#include "scaler/io/ymq/common.h"

class FileDescriptor {
    int fd;

    FileDescriptor(int fd): fd(fd) {}

public:
    ~FileDescriptor() noexcept(false)
    {
#ifdef _WIN32
        if (fd >= 0 && closesocket(fd) < 0)
            throw std::system_error(WSAGetLastError(), std::system_category(), "Failed to close file descriptor");
#else
        if (fd >= 0 && close(fd) < 0)
            throw std::system_error(errno, std::system_category(), "Failed to close file descriptor");
#endif
        this->fd = -1;
    }

    FileDescriptor(): fd(-1) {}

    // move-only
    FileDescriptor(const FileDescriptor&)            = delete;
    FileDescriptor& operator=(const FileDescriptor&) = delete;
    FileDescriptor(FileDescriptor&& other) noexcept: fd(other.fd)
    {
        other.fd = -1;  // prevent double close
    }
    FileDescriptor& operator=(FileDescriptor&& other) noexcept
    {
        if (this != &other) {
            if (fd >= 0) {
#ifdef _WIN32
                closesocket(fd);
#else
                close(fd);  // close current fd
#endif
            }
            fd       = other.fd;
            other.fd = -1;  // prevent double close
        }
        return *this;
    }

    static std::expected<FileDescriptor, Errno> socket(int domain, int type, int protocol)
    {
        int fd = ::socket(domain, type, protocol);
        if (fd < 0) {
#ifdef _WIN32
            return std::unexpected {WSAGetLastError()};
#else
            return std::unexpected {errno};
#endif
        } else {
            return FileDescriptor(fd);
        }
    }

#ifdef __linux__
    static std::expected<FileDescriptor, Errno> eventfd(int initval, int flags)
    {
        int fd = ::eventfd(initval, flags);
        if (fd < 0) {
            return std::unexpected {errno};
        } else {
            return FileDescriptor(fd);
        }
    }

    static std::expected<FileDescriptor, Errno> timerfd(clockid_t clock, int flags)
    {
        int fd = ::timerfd_create(clock, flags);
        if (fd < 0) {
            return std::unexpected {errno};
        } else {
            return FileDescriptor(fd);
        }
    }

    static std::expected<FileDescriptor, Errno> epollfd()
    {
        int fd = ::epoll_create1(0);
        if (fd < 0) {
            return std::unexpected {errno};
        } else {
            return FileDescriptor(fd);
        }
    }
#endif  // __linux__

#ifdef __APPLE__
    static std::expected<FileDescriptor, Errno> kqueuefd()
    {
        int fd = ::kqueue();
        if (fd < 0) {
            return std::unexpected {errno};
        } else {
            return FileDescriptor(fd);
        }
    }
#endif  // __APPLE__

    std::optional<Errno> listen(int backlog)
    {
        if (::listen(fd, backlog) < 0) {
#ifdef _WIN32
            return WSAGetLastError();
#else
            return errno;
#endif
        } else {
            return std::nullopt;
        }
    }

    std::expected<FileDescriptor, Errno> accept(sockaddr& addr, socklen_t& addrlen)
    {
        int fd2 = ::accept(fd, &addr, &addrlen);
        if (fd2 < 0) {
#ifdef _WIN32
            return std::unexpected {WSAGetLastError()};
#else
            return std::unexpected {errno};
#endif
        } else {
            return FileDescriptor(fd2);
        }
    }

    std::optional<Errno> connect(const sockaddr& addr, socklen_t addrlen)
    {
        if (::connect(fd, &addr, addrlen) < 0) {
#ifdef _WIN32
            return WSAGetLastError();
#else
            return errno;
#endif
        } else {
            return std::nullopt;
        }
    }

    std::optional<Errno> bind(const sockaddr& addr, socklen_t addrlen)
    {
        if (::bind(fd, &addr, addrlen) < 0) {
#ifdef _WIN32
            return WSAGetLastError();
#else
            return errno;
#endif
        } else {
            return std::nullopt;
        }
    }

#ifndef _WIN32
    std::expected<ssize_t, Errno> read(void* buf, size_t count)
    {
        ssize_t n = ::read(fd, buf, count);
        if (n < 0) {
            return std::unexpected {errno};
        } else {
            return n;
        }
    }

    std::expected<ssize_t, Errno> write(const void* buf, size_t count)
    {
        ssize_t n = ::write(fd, buf, count);
        if (n < 0) {
            return std::unexpected {errno};
        } else {
            return n;
        }
    }
#endif  // !_WIN32

#ifdef __linux__
    std::optional<Errno> eventfd_signal()
    {
        uint64_t u = 1;
        if (::eventfd_write(fd, u) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::optional<Errno> eventfd_wait()
    {
        uint64_t u;
        if (::eventfd_read(fd, &u) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::optional<Errno> timerfd_settime(const itimerspec& new_value, itimerspec* old_value = nullptr)
    {
        if (::timerfd_settime(fd, 0, &new_value, old_value) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::optional<Errno> timerfd_wait()
    {
        uint64_t u;
        if (::read(fd, &u, sizeof(u)) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::optional<Errno> epoll_ctl(int op, FileDescriptor& other, epoll_event* event)
    {
        if (::epoll_ctl(fd, op, other.fd, event) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::expected<int, Errno> epoll_wait(epoll_event* events, int maxevents, int timeout)
    {
        int n = ::epoll_wait(fd, events, maxevents, timeout);
        if (n < 0) {
            return std::unexpected {errno};
        } else {
            return n;
        }
    }
#endif  // __linux__

#ifdef __APPLE__
    std::optional<Errno> kevent_ctl(const struct kevent* changelist, int nchanges)
    {
        if (::kevent(fd, changelist, nchanges, nullptr, 0, nullptr) < 0) {
            return errno;
        } else {
            return std::nullopt;
        }
    }

    std::expected<int, Errno> kevent_wait(struct kevent* eventlist, int nevents, const struct timespec* timeout)
    {
        int n = ::kevent(fd, nullptr, 0, eventlist, nevents, timeout);
        if (n < 0) {
            return std::unexpected {errno};
        } else {
            return n;
        }
    }
#endif  // __APPLE__
};
