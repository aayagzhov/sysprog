#include "chat.h"
#include "chat_client.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <string>
#include <unistd.h>
#include <vector>

struct chat_client {
	/** Socket connected to the server. */
	int socket = -1;
	/** Received but not yet popped messages. */
	std::vector<chat_message *> in_msgs;
	/** Input accumulation buffer (partial message from server). */
	std::string in_buf;
	/** Output buffer: data to be sent to the server. */
	std::string out_buf;
	/**
	 * Feed accumulation buffer: partial line from chat_client_feed()
	 * calls that haven't seen '\n' yet. Per-client, not static.
	 */
	std::string feed_buf;
#if NEED_AUTHOR
	/** This client's name, sent once on connect. */
	std::string name;
#endif
};

struct chat_client *
chat_client_new(std::string_view name)
{
	chat_client *c = new chat_client();
#if NEED_AUTHOR
	c->name = std::string(name);
#else
	(void)name;
#endif
	return c;
}

void
chat_client_delete(struct chat_client *client)
{
	if (client->socket >= 0)
		close(client->socket);
	for (chat_message *m : client->in_msgs)
		delete m;
	delete client;
}

int
chat_client_connect(struct chat_client *client, std::string_view addr)
{
	if (client->socket >= 0)
		return CHAT_ERR_ALREADY_STARTED;

	/* Split addr into host:port */
	std::string addr_str(addr);
	size_t colon = addr_str.rfind(':');
	if (colon == std::string::npos)
		return CHAT_ERR_NO_ADDR;
	std::string host = addr_str.substr(0, colon);
	std::string port_str = addr_str.substr(colon + 1);

	struct addrinfo hints;
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;

	struct addrinfo *res = nullptr;
	int rc = getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res);
	if (rc != 0 || res == nullptr)
		return CHAT_ERR_NO_ADDR;

	int sock = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
	if (sock < 0) {
		freeaddrinfo(res);
		return CHAT_ERR_SYS;
	}

	/* Blocking connect is allowed per task rules. */
	rc = connect(sock, res->ai_addr, res->ai_addrlen);
	freeaddrinfo(res);
	if (rc != 0) {
		close(sock);
		return CHAT_ERR_SYS;
	}

	/* Make non-blocking after connect. */
	int flags = fcntl(sock, F_GETFL, 0);
	if (flags < 0 || fcntl(sock, F_SETFL, flags | O_NONBLOCK) < 0) {
		close(sock);
		return CHAT_ERR_SYS;
	}

	client->socket = sock;

#if NEED_AUTHOR
	/* Queue the name as the very first message: "name\n" */
	client->out_buf += client->name;
	client->out_buf += '\n';
#endif

	return 0;
}

struct chat_message *
chat_client_pop_next(struct chat_client *client)
{
	if (client->in_msgs.empty())
		return nullptr;
	chat_message *m = client->in_msgs.front();
	client->in_msgs.erase(client->in_msgs.begin());
	return m;
}

/**
 * Parse in_buf: extract complete '\n'-terminated messages, trim whitespace,
 * skip blank ones, push to in_msgs.
 * When NEED_AUTHOR: wire format is "author\nbody\n", so alternate lines.
 */
static void
client_parse_input(struct chat_client *client)
{
	std::string &buf = client->in_buf;
	size_t pos = 0;

#if NEED_AUTHOR
	while (true) {
		/* Read author line. */
		size_t nl1 = buf.find('\n', pos);
		if (nl1 == std::string::npos)
			break;
		std::string author = buf.substr(pos, nl1 - pos);
		size_t pos2 = nl1 + 1;

		/* Read body line. */
		size_t nl2 = buf.find('\n', pos2);
		if (nl2 == std::string::npos)
			break;
		std::string body = buf.substr(pos2, nl2 - pos2);
		pos = nl2 + 1;

		/* Trim body. */
		size_t start = 0;
		while (start < body.size() && isspace((unsigned char)body[start]))
			++start;
		size_t end = body.size();
		while (end > start && isspace((unsigned char)body[end - 1]))
			--end;
		if (start >= end)
			continue;

		chat_message *m = new chat_message();
		m->author = author;
		m->data = body.substr(start, end - start);
		client->in_msgs.push_back(m);
	}
#else
	while (true) {
		size_t nl = buf.find('\n', pos);
		if (nl == std::string::npos)
			break;
		std::string line = buf.substr(pos, nl - pos);
		pos = nl + 1;

		/* Trim leading whitespace. */
		size_t start = 0;
		while (start < line.size() && isspace((unsigned char)line[start]))
			++start;
		/* Trim trailing whitespace. */
		size_t end = line.size();
		while (end > start && isspace((unsigned char)line[end - 1]))
			--end;

		if (start >= end)
			continue;

		chat_message *m = new chat_message();
		m->data = line.substr(start, end - start);
		client->in_msgs.push_back(m);
	}
#endif
	buf.erase(0, pos);
}

int
chat_client_update(struct chat_client *client, double timeout)
{
	if (client->socket < 0)
		return CHAT_ERR_NOT_STARTED;

	struct pollfd pfd;
	pfd.fd = client->socket;
	pfd.events = chat_events_to_poll_events(chat_client_get_events(client));
	pfd.revents = 0;

	int timeout_ms = (timeout < 0) ? -1 : (int)(timeout * 1000);
	int rc = poll(&pfd, 1, timeout_ms);
	if (rc < 0)
		return CHAT_ERR_SYS;
	if (rc == 0)
		return CHAT_ERR_TIMEOUT;

	/* Write pending output. */
	if ((pfd.revents & POLLOUT) != 0 && !client->out_buf.empty()) {
		while (!client->out_buf.empty()) {
			ssize_t sent = send(client->socket, client->out_buf.data(),
					    client->out_buf.size(), 0);
			if (sent > 0) {
				client->out_buf.erase(0, (size_t)sent);
			} else if (sent < 0 &&
				   (errno == EAGAIN || errno == EWOULDBLOCK)) {
				break;
			} else {
				break;
			}
		}
	}

	/* Read incoming data. */
	if ((pfd.revents & POLLIN) != 0) {
		char buf[4096];
		while (true) {
			ssize_t n = recv(client->socket, buf, sizeof(buf), 0);
			if (n > 0) {
				client->in_buf.append(buf, (size_t)n);
			} else if (n == 0) {
				/* Server closed connection. */
				break;
			} else {
				/* EWOULDBLOCK / EAGAIN - done for now. */
				break;
			}
		}
		client_parse_input(client);
	}

	return 0;
}

int
chat_client_get_descriptor(const struct chat_client *client)
{
	return client->socket;
}

int
chat_client_get_events(const struct chat_client *client)
{
	if (client->socket < 0)
		return 0;
	int events = CHAT_EVENT_INPUT;
	if (!client->out_buf.empty())
		events |= CHAT_EVENT_OUTPUT;
	return events;
}

int
chat_client_feed(struct chat_client *client, const char *msg, uint32_t msg_size)
{
	if (client->socket < 0)
		return CHAT_ERR_NOT_STARTED;

	/*
	 * Append to per-client feed accumulation buffer, then parse complete
	 * '\n'-terminated lines: trim whitespace, skip blank, enqueue to
	 * out_buf for sending.
	 */
	client->feed_buf.append(msg, msg_size);

	size_t pos = 0;
	while (true) {
		size_t nl = client->feed_buf.find('\n', pos);
		if (nl == std::string::npos)
			break;
		std::string line = client->feed_buf.substr(pos, nl - pos);
		pos = nl + 1;

		/* Trim leading whitespace. */
		size_t start = 0;
		while (start < line.size() && isspace((unsigned char)line[start]))
			++start;
		/* Trim trailing whitespace. */
		size_t end = line.size();
		while (end > start && isspace((unsigned char)line[end - 1]))
			--end;

		if (start >= end)
			continue; /* Skip blank. */

		std::string trimmed = line.substr(start, end - start);
		client->out_buf += trimmed;
		client->out_buf += '\n';
	}
	client->feed_buf.erase(0, pos);

	return 0;
}
