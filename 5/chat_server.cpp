#include "chat.h"
#include "chat_server.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <string>
#include <sys/event.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

/* ------------------------------------------------------------------ */
/* chat_peer - один подключённый клиент                                */
/* ------------------------------------------------------------------ */

struct chat_peer {
	/** Сокет клиента. */
	int socket = -1;
	/** Буфер вывода: данные, ожидающие отправки этому пиру. */
	std::string out_buf;
	/** Буфер ввода: частичное сообщение от этого пира. */
	std::string in_buf;
#if NEED_AUTHOR
	/** Имя автора, полученное от этого пира. */
	std::string author;
	/** Получено ли имя автора. */
	bool author_received = false;
#endif
};

/* ------------------------------------------------------------------ */
/* chat_server                                                          */
/* ------------------------------------------------------------------ */

struct chat_server {
	/** Слушающий сокет. */
	int socket = -1;
	/** Дескриптор kqueue. */
	int kq = -1;
	/** Все подключённые пиры. */
	std::vector<chat_peer *> peers;
	/** Полученные сообщения (от клиентов или от сервера). */
	std::vector<chat_message *> in_msgs;
#if NEED_SERVER_FEED
	/** Буфер накопления для серверного ввода. */
	std::string server_in_buf;
	/**
	 * Очередь серверных сообщений, ожидающих рассылки.
	 * chat_server_feed() добавляет сюда, chat_server_update()
	 * рассылает при следующем вызове (когда пиры уже приняты).
	 */
	std::vector<std::string> pending_feed;
#endif
};

/* ------------------------------------------------------------------ */
/* Вспомогательные функции                                             */
/* ------------------------------------------------------------------ */

static int
set_nonblock(int fd)
{
	int flags = fcntl(fd, F_GETFL, 0);
	if (flags < 0)
		return -1;
	return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/**
 * Регистрируем слушающий сокет в kqueue только на чтение с EV_CLEAR.
 */
static int
kq_add_listen(int kq, int fd)
{
	struct kevent ev;
	EV_SET(&ev, (uintptr_t)fd, EVFILT_READ, EV_ADD | EV_CLEAR, 0, 0, nullptr);
	return kevent(kq, &ev, 1, nullptr, 0, nullptr);
}

/**
 * Регистрируем клиентский сокет в kqueue:
 *   - EVFILT_READ:  level-triggered (без EV_CLEAR) — не пропускаем данные.
 *   - EVFILT_WRITE: edge-triggered (EV_CLEAR) — не крутимся в busy-loop.
 */
static int
kq_add_peer(int kq, int fd, void *udata)
{
	struct kevent evs[2];
	EV_SET(&evs[0], (uintptr_t)fd, EVFILT_READ,  EV_ADD,            0, 0, udata);
	EV_SET(&evs[1], (uintptr_t)fd, EVFILT_WRITE, EV_ADD | EV_CLEAR, 0, 0, udata);
	return kevent(kq, evs, 2, nullptr, 0, nullptr);
}

/**
 * Удаляем слушающий сокет из kqueue перед закрытием.
 */
static void
kq_del_listen(int kq, int fd)
{
	struct kevent ev;
	EV_SET(&ev, (uintptr_t)fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
	kevent(kq, &ev, 1, nullptr, 0, nullptr);
}

/**
 * Удаляем клиентский сокет из kqueue перед закрытием.
 */
static void
kq_del_peer(int kq, int fd)
{
	struct kevent evs[2];
	EV_SET(&evs[0], (uintptr_t)fd, EVFILT_READ,  EV_DELETE, 0, 0, nullptr);
	EV_SET(&evs[1], (uintptr_t)fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
	kevent(kq, evs, 2, nullptr, 0, nullptr);
}

/**
 * Обрезаем пробельные символы слева и справа.
 */
static std::string
trim(const std::string &s)
{
	size_t start = 0;
	while (start < s.size() && isspace((unsigned char)s[start]))
		++start;
	size_t end = s.size();
	while (end > start && isspace((unsigned char)s[end - 1]))
		--end;
	return s.substr(start, end - start);
}

/**
 * Пытаемся отправить данные из out_buf пира.
 * Возвращает false, если пир должен быть удалён (ошибка сокета).
 */
static bool
peer_flush(chat_peer *peer)
{
	while (!peer->out_buf.empty()) {
		ssize_t sent = send(peer->socket, peer->out_buf.data(),
				    peer->out_buf.size(), 0);
		if (sent > 0) {
			peer->out_buf.erase(0, (size_t)sent);
		} else if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
			/* Буфер ядра заполнен — ждём EVFILT_WRITE. */
			break;
		} else {
			/* Ошибка или соединение закрыто. */
			return false;
		}
	}
	return true;
}

/**
 * Рассылаем сообщение всем пирам кроме отправителя.
 * Формат по проводу: "author\nbody\n" (NEED_AUTHOR) или "body\n".
 * После добавления в out_buf сразу пытаемся отправить — с EV_CLEAR
 * на EVFILT_WRITE событие придёт только при переходе "не мог" → "может",
 * поэтому нужно отправлять жадно.
 */
static void
broadcast(struct chat_server *server, chat_peer *sender,
	  const std::string &author, const std::string &body)
{
	std::string wire;
#if NEED_AUTHOR
	wire = author + '\n' + body + '\n';
#else
	(void)author;
	wire = body + '\n';
#endif

	for (chat_peer *p : server->peers) {
		if (p == sender)
			continue;
		p->out_buf += wire;
		/* Жадная отправка. */
		peer_flush(p);
	}
}

/**
 * Удаляем пира: убираем из kqueue, закрываем сокет, освобождаем память.
 */
static void
peer_remove(struct chat_server *server, chat_peer *peer)
{
	kq_del_peer(server->kq, peer->socket);
	close(peer->socket);
	for (auto it = server->peers.begin(); it != server->peers.end(); ++it) {
		if (*it == peer) {
			server->peers.erase(it);
			break;
		}
	}
	delete peer;
}

/**
 * Разбираем in_buf пира: извлекаем полные сообщения, добавляем в
 * server->in_msgs и рассылаем другим пирам.
 */
static void
peer_parse_input(struct chat_server *server, chat_peer *peer)
{
	std::string &buf = peer->in_buf;
	size_t pos = 0;

	while (true) {
		size_t nl = buf.find('\n', pos);
		if (nl == std::string::npos)
			break;
		std::string line = buf.substr(pos, nl - pos);
		pos = nl + 1;

#if NEED_AUTHOR
		if (!peer->author_received) {
			/* Первое сообщение — имя автора. */
			peer->author = line;
			peer->author_received = true;
			continue;
		}
#endif

		std::string body = trim(line);
		if (body.empty())
			continue;

		/* Добавляем в очередь сервера. */
		chat_message *m = new chat_message();
		m->data = body;
#if NEED_AUTHOR
		m->author = peer->author;
#endif
		server->in_msgs.push_back(m);

		/* Рассылаем всем остальным. */
#if NEED_AUTHOR
		broadcast(server, peer, peer->author, body);
#else
		broadcast(server, peer, "", body);
#endif
	}
	buf.erase(0, pos);
}

/* ------------------------------------------------------------------ */
/* Публичный API                                                        */
/* ------------------------------------------------------------------ */

struct chat_server *
chat_server_new(void)
{
	return new chat_server();
}

void
chat_server_delete(struct chat_server *server)
{
	/* Удаляем всех пиров. */
	while (!server->peers.empty()) {
		chat_peer *p = server->peers.back();
		server->peers.pop_back();
		kq_del_peer(server->kq, p->socket);
		close(p->socket);
		delete p;
	}

	if (server->socket >= 0) {
		if (server->kq >= 0)
			kq_del_listen(server->kq, server->socket);
		close(server->socket);
	}

	if (server->kq >= 0)
		close(server->kq);

	/* Освобождаем ожидающие сообщения. */
	for (chat_message *m : server->in_msgs)
		delete m;

	delete server;
}

int
chat_server_listen(struct chat_server *server, uint16_t port)
{
	if (server->socket >= 0)
		return CHAT_ERR_ALREADY_STARTED;

	int sock = socket(AF_INET, SOCK_STREAM, 0);
	if (sock < 0)
		return CHAT_ERR_SYS;

	int opt = 1;
	setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

	struct sockaddr_in addr;
	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	addr.sin_addr.s_addr = htonl(INADDR_ANY);

	if (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
		int err = errno;
		close(sock);
		return (err == EADDRINUSE) ? CHAT_ERR_PORT_BUSY : CHAT_ERR_SYS;
	}

	if (listen(sock, SOMAXCONN) < 0) {
		close(sock);
		return CHAT_ERR_SYS;
	}

	if (set_nonblock(sock) < 0) {
		close(sock);
		return CHAT_ERR_SYS;
	}

	int kq = kqueue();
	if (kq < 0) {
		close(sock);
		abort();
	}

	server->socket = sock;
	server->kq = kq;

	if (kq_add_listen(kq, sock) < 0) {
		close(sock);
		close(kq);
		server->socket = -1;
		server->kq = -1;
		return CHAT_ERR_SYS;
	}

	return 0;
}

struct chat_message *
chat_server_pop_next(struct chat_server *server)
{
	if (server->in_msgs.empty())
		return nullptr;
	chat_message *m = server->in_msgs.front();
	server->in_msgs.erase(server->in_msgs.begin());
	return m;
}

int
chat_server_update(struct chat_server *server, double timeout)
{
	if (server->socket < 0)
		return CHAT_ERR_NOT_STARTED;

	struct timespec ts;
	struct timespec *pts = nullptr;
	if (timeout >= 0) {
		/*
		 * Используем минимум 1ms при timeout==0, чтобы ядро успело
		 * доставить kqueue-события для данных, только что записанных
		 * в loopback-сокет. Чистый {0,0} poll может пропустить
		 * события, которые ещё не поставлены в очередь.
		 */
		double effective = (timeout == 0.0) ? 0.001 : timeout;
		ts.tv_sec  = (time_t)effective;
		ts.tv_nsec = (long)((effective - (double)ts.tv_sec) * 1e9);
		pts = &ts;
	}

	const int MAX_EVENTS = 64;
	struct kevent events[MAX_EVENTS];

	/* Жадный flush всех пиров с непустым out_buf — независимо от kqueue-событий.
	 * С EV_CLEAR на EVFILT_WRITE событие не придёт если сокет всегда был writable. */
	for (chat_peer *p : server->peers) {
		if (!p->out_buf.empty())
			peer_flush(p);
	}

	int n = kevent(server->kq, nullptr, 0, events, MAX_EVENTS, pts);
	if (n < 0)
		return CHAT_ERR_SYS;

#if NEED_SERVER_FEED
	/*
	 * Рассылаем накопленные серверные сообщения всем текущим пирам.
	 * Делаем это ПОСЛЕ kevent(), чтобы новые пиры (принятые через
	 * accept() внутри цикла ниже) тоже получили сообщения.
	 * Если kevent() вернул 0 (timeout) но pending_feed непуст —
	 * не возвращаем CHAT_ERR_TIMEOUT, а сначала рассылаем.
	 */
	bool has_pending = !server->pending_feed.empty();
#endif

	if (n == 0) {
#if NEED_SERVER_FEED
		if (!has_pending)
#endif
			return CHAT_ERR_TIMEOUT;
	}

	for (int i = 0; i < n; ++i) {
		struct kevent *ev = &events[i];
		chat_peer *peer = (chat_peer *)ev->udata;

		if (ev->flags & EV_ERROR)
			continue;

		if (peer == nullptr) {
			/* Событие на слушающем сокете. */
			if (ev->filter == EVFILT_READ) {
				while (true) {
					struct sockaddr_in caddr;
					socklen_t clen = sizeof(caddr);
					int csock = accept(server->socket,
							   (struct sockaddr *)&caddr,
							   &clen);
					if (csock < 0)
						break;
					if (set_nonblock(csock) < 0) {
						close(csock);
						continue;
					}
					chat_peer *p = new chat_peer();
					p->socket = csock;
					server->peers.push_back(p);
					if (kq_add_peer(server->kq, csock, p) < 0) {
						server->peers.pop_back();
						close(csock);
						delete p;
						continue;
					}
					/*
					 * Жадное чтение сразу после accept:
					 * клиент мог отправить данные до того,
					 * как мы зарегистрировали сокет в kqueue.
					 */
					{
						bool dead = false;
						char buf[4096];
						while (true) {
							ssize_t nr = recv(csock, buf,
									  sizeof(buf), 0);
							if (nr > 0) {
								p->in_buf.append(buf, (size_t)nr);
							} else if (nr == 0) {
								dead = true;
								break;
							} else {
								break;
							}
						}
						peer_parse_input(server, p);
						if (dead)
							peer_remove(server, p);
					}
				}
			}
		} else {
			/* Событие на клиентском сокете. */
			if (ev->filter == EVFILT_WRITE) {
				if (!peer_flush(peer)) {
					peer_remove(server, peer);
					continue;
				}
			}
			if (ev->filter == EVFILT_READ) {
				bool dead = false;
				char buf[4096];
				while (true) {
					ssize_t nr = recv(peer->socket, buf,
							  sizeof(buf), 0);
					if (nr > 0) {
						peer->in_buf.append(buf, (size_t)nr);
					} else if (nr == 0) {
						dead = true;
						break;
					} else {
						break;
					}
				}
				peer_parse_input(server, peer);
				if (dead) {
					peer_remove(server, peer);
				}
			}
		}
	}

#if NEED_SERVER_FEED
	/*
	 * Рассылаем накопленные серверные сообщения всем текущим пирам.
	 * Делаем это ПОСЛЕ цикла kevent, чтобы новые пиры (принятые через
	 * accept() выше) тоже получили сообщения.
	 */
	if (!server->pending_feed.empty()) {
		std::vector<std::string> to_send;
		to_send.swap(server->pending_feed);
		for (const std::string &body : to_send)
			broadcast(server, nullptr, "server", body);
	}
#endif

	return 0;
}

int
chat_server_get_descriptor(const struct chat_server *server)
{
#if NEED_SERVER_FEED
	return server->kq;
#else
	(void)server;
	return -1;
#endif
}

int
chat_server_get_socket(const struct chat_server *server)
{
	return server->socket;
}

int
chat_server_get_events(const struct chat_server *server)
{
	if (server->socket < 0)
		return 0;
	int events = CHAT_EVENT_INPUT;
	for (const chat_peer *p : server->peers) {
		if (!p->out_buf.empty()) {
			events |= CHAT_EVENT_OUTPUT;
			break;
		}
	}
	return events;
}

int
chat_server_feed(struct chat_server *server, const char *msg, uint32_t msg_size)
{
#if NEED_SERVER_FEED
	if (server->socket < 0)
		return CHAT_ERR_NOT_STARTED;

	server->server_in_buf.append(msg, msg_size);

	size_t pos = 0;
	while (true) {
		size_t nl = server->server_in_buf.find('\n', pos);
		if (nl == std::string::npos)
			break;
		std::string line = server->server_in_buf.substr(pos, nl - pos);
		pos = nl + 1;

		std::string body = trim(line);
		if (body.empty())
			continue;

		chat_message *m = new chat_message();
		m->data = body;
#if NEED_AUTHOR
		m->author = "server";
#endif
		server->in_msgs.push_back(m);

		/*
		 * Добавляем в очередь ожидающих рассылки.
		 * chat_server_update() разошлёт их при следующем вызове,
		 * когда пиры уже будут приняты через accept().
		 */
		server->pending_feed.push_back(body);
	}
	server->server_in_buf.erase(0, pos);

	return 0;
#else
	(void)server;
	(void)msg;
	(void)msg_size;
	return CHAT_ERR_NOT_IMPLEMENTED;
#endif
}
