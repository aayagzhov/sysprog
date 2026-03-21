#include "chat.h"
#include "chat_server.h"

#include <errno.h>
#include <poll.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int
port_from_str(const char *str, uint16_t *port)
{
	errno = 0;
	char *end = NULL;
	long res = strtol(str, &end, 10);
	if (res == 0 && errno != 0)
		return -1;
	if (*end != 0)
		return -1;
	if (res > UINT16_MAX || res < 0)
		return -1;
	*port = (uint16_t)res;
	return 0;
}

int
main(int argc, char **argv)
{
	if (argc < 2) {
		printf("Expected a port to listen on\n");
		return -1;
	}
	uint16_t port = 0;
	int rc = port_from_str(argv[1], &port);
	if (rc != 0) {
		printf("Invalid port\n");
		return -1;
	}
	struct chat_server *serv = chat_server_new();
	rc = chat_server_listen(serv, port);
	if (rc != 0) {
		printf("Couldn't listen: %d\n", rc);
		chat_server_delete(serv);
		return -1;
	}
#if NEED_SERVER_FEED
	/*
	 * Two pollfd objects:
	 *   [0] - STDIN_FILENO for server admin input
	 *   [1] - chat_server_get_descriptor() (kqueue fd) for client events
	 */
	struct pollfd poll_fds[2];
	memset(poll_fds, 0, sizeof(poll_fds));

	struct pollfd *poll_stdin = &poll_fds[0];
	poll_stdin->fd = STDIN_FILENO;
	poll_stdin->events = POLLIN;

	struct pollfd *poll_server = &poll_fds[1];
	poll_server->fd = chat_server_get_descriptor(serv);
	poll_server->events =
		chat_events_to_poll_events(chat_server_get_events(serv));

	const int buf_size = 4096;
	char buf[buf_size];

	while (true) {
		poll_server->events =
			chat_events_to_poll_events(chat_server_get_events(serv));

		rc = poll(poll_fds, 2, -1);
		if (rc < 0) {
			printf("Poll error: %d\n", errno);
			break;
		}

		if (poll_stdin->revents != 0) {
			poll_stdin->revents = 0;
			int n = read(STDIN_FILENO, buf, buf_size - 1);
			if (n == 0) {
				printf("EOF - exiting\n");
				break;
			}
			if (n > 0) {
				rc = chat_server_feed(serv, buf, n);
				if (rc != 0) {
					printf("Server feed error: %d\n", rc);
					break;
				}
			}
		}

		if (poll_server->revents != 0) {
			poll_server->revents = 0;
			rc = chat_server_update(serv, 0);
			if (rc != 0 && rc != CHAT_ERR_TIMEOUT) {
				printf("Update error: %d\n", rc);
				break;
			}
		}

		/* Flush all the pending messages to the standard output. */
		struct chat_message *msg;
		while ((msg = chat_server_pop_next(serv)) != NULL) {
#if NEED_AUTHOR
			printf("%s: %s\n", msg->author.c_str(), msg->data.c_str());
#else
			printf("%s\n", msg->data.c_str());
#endif
			delete msg;
		}
	}
#else
	/*
	 * The basic implementation without server messages. Just serving
	 * clients.
	 */
	while (true) {
		int rc = chat_server_update(serv, -1);
		if (rc != 0) {
			printf("Update error: %d\n", rc);
			break;
		}
		/* Flush all the pending messages to the standard output. */
		struct chat_message *msg;
		while ((msg = chat_server_pop_next(serv)) != NULL) {
#if NEED_AUTHOR
			printf("%s: %s\n", msg->author.c_str(), msg->data.c_str());
#else
			printf("%s\n", msg->data.c_str());
#endif
			delete msg;
		}
	}
#endif
	chat_server_delete(serv);
	return 0;
}
