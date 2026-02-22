#include "parser.h"

#include <assert.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <vector>
#include <string>
#include <memory>

// Exception to handle clean exit and stack unwinding
struct ExitException {
    int code;
    ExitException(int c) : code(c) {}
};

// RAII wrapper for parser
struct ParserGuard {
    parser *p;
    ParserGuard() { p = parser_new(); }
    ~ParserGuard() { if (p) parser_delete(p); }
    operator parser*() { return p; }
};

// RAII wrapper for command_line
struct LineGuard {
    command_line *line;
    LineGuard() : line(NULL) {}
    ~LineGuard() { if (line) delete line; }
    command_line** ptr() { return &line; }
    command_line* get() { return line; }
    command_line* operator->() { return line; }
};

struct Job {
    std::vector<expr> exprs;
    enum expr_type next_op;
};

static std::vector<Job>
split_jobs(const std::list<expr>& exprs)
{
    std::vector<Job> jobs;
    Job current_job;
    current_job.next_op = EXPR_TYPE_COMMAND;

    for (const auto& e : exprs) {
        if (e.type == EXPR_TYPE_AND || e.type == EXPR_TYPE_OR) {
            current_job.next_op = e.type;
            jobs.push_back(current_job);
            current_job = Job();
        } else {
            current_job.exprs.push_back(e);
        }
    }
    if (!current_job.exprs.empty()) {
        current_job.next_op = EXPR_TYPE_COMMAND;
        jobs.push_back(current_job);
    }
    return jobs;
}

static int
execute_pipeline(const std::vector<expr>& exprs, const struct command_line *line, bool apply_redirection)
{
    if (exprs.empty())
        return 0;

    // Check for single built-in commands first
    if (exprs.size() == 1 && exprs.front().type == EXPR_TYPE_COMMAND) {
        const command& cmd = *exprs.front().cmd;
        if (cmd.exe == "cd") {
            if (cmd.args.size() > 1) {
                fprintf(stderr, "cd: too many arguments\n");
                return 1;
            } else if (cmd.args.size() == 1) {
                if (chdir(cmd.args[0].c_str()) != 0) {
                    perror("cd");
                    return 1;
                }
            } else {
                const char* home = getenv("HOME");
                if (home) {
                    if (chdir(home) != 0) {
                        perror("cd");
                        return 1;
                    }
                }
            }
            return 0;
        } else if (cmd.exe == "exit") {
            int exit_code = 0;
            if (cmd.args.size() >= 1) {
                exit_code = std::atoi(cmd.args[0].c_str());
            }
            throw ExitException(exit_code);
        }
    }

    // Handle pipeline
    int prev_pipe_read = -1;
    std::vector<pid_t> pids;
    
    auto it = exprs.begin();
    while (it != exprs.end()) {
        if (it->type == EXPR_TYPE_COMMAND) {
            const command& cmd = *it->cmd;
            
            auto next_it = std::next(it);
            bool is_pipe = (next_it != exprs.end() && next_it->type == EXPR_TYPE_PIPE);
            
            int pipe_fds[2] = {-1, -1};
            if (is_pipe) {
                if (pipe(pipe_fds) == -1) {
                    perror("pipe");
                    return 1;
                }
            }

            pid_t pid = fork();
            if (pid == -1) {
                perror("fork");
                if (is_pipe) {
                    close(pipe_fds[0]);
                    close(pipe_fds[1]);
                }
                if (prev_pipe_read != -1) {
                    close(prev_pipe_read);
                }
                return 1;
            }

            if (pid == 0) {
                // Child process
                
                if (prev_pipe_read != -1) {
                    if (dup2(prev_pipe_read, STDIN_FILENO) == -1) {
                        perror("dup2");
                        throw ExitException(EXIT_FAILURE);
                    }
                    close(prev_pipe_read);
                }

                if (is_pipe) {
                    if (dup2(pipe_fds[1], STDOUT_FILENO) == -1) {
                        perror("dup2");
                        throw ExitException(EXIT_FAILURE);
                    }
                    close(pipe_fds[1]);
                    close(pipe_fds[0]);
                } else {
                    // Last command in the pipeline
                    if (apply_redirection && line->out_type != OUTPUT_TYPE_STDOUT) {
                        int fd = -1;
                        if (line->out_type == OUTPUT_TYPE_FILE_NEW) {
                            fd = open(line->out_file.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
                        } else if (line->out_type == OUTPUT_TYPE_FILE_APPEND) {
                            fd = open(line->out_file.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0666);
                        }
                        
                        if (fd == -1) {
                            perror("open");
                            throw ExitException(EXIT_FAILURE);
                        }
                        
                        if (dup2(fd, STDOUT_FILENO) == -1) {
                            perror("dup2");
                            throw ExitException(EXIT_FAILURE);
                        }
                        close(fd);
                    }
                }

                // Handle built-ins in child
                if (cmd.exe == "exit") {
                    int exit_code = 0;
                    if (cmd.args.size() >= 1) {
                        exit_code = std::atoi(cmd.args[0].c_str());
                    }
                    throw ExitException(exit_code);
                } else if (cmd.exe == "cd") {
                    if (cmd.args.size() > 1) {
                        fprintf(stderr, "cd: too many arguments\n");
                        throw ExitException(EXIT_FAILURE);
                    } else if (cmd.args.size() == 1) {
                        if (chdir(cmd.args[0].c_str()) != 0) {
                            perror("cd");
                            throw ExitException(EXIT_FAILURE);
                        }
                    } else {
                        const char* home = getenv("HOME");
                        if (home) {
                            if (chdir(home) != 0) {
                                perror("cd");
                                throw ExitException(EXIT_FAILURE);
                            }
                        }
                    }
                    throw ExitException(EXIT_SUCCESS);
                }

                std::vector<char*> args;
                args.push_back(const_cast<char*>(cmd.exe.c_str()));
                for (const std::string& arg : cmd.args) {
                    args.push_back(const_cast<char*>(arg.c_str()));
                }
                args.push_back(NULL);

                execvp(cmd.exe.c_str(), args.data());
                perror("execvp");
                throw ExitException(EXIT_FAILURE);
            } else {
                // Parent process
                pids.push_back(pid);
                
                if (prev_pipe_read != -1) {
                    close(prev_pipe_read);
                }
                
                if (is_pipe) {
                    prev_pipe_read = pipe_fds[0];
                    close(pipe_fds[1]);
                } else {
                    prev_pipe_read = -1;
                }
            }
            
            if (is_pipe) {
                it = std::next(it); // Skip PIPE
            }
        }
        ++it;
    }

    int last_status = 0;
    for (size_t i = 0; i < pids.size(); ++i) {
        int status;
        waitpid(pids[i], &status, 0);
        if (i == pids.size() - 1) {
            if (WIFEXITED(status)) {
                last_status = WEXITSTATUS(status);
            } else if (WIFSIGNALED(status)) {
                last_status = 128 + WTERMSIG(status);
            }
        }
    }
    return last_status;
}

static int
execute_jobs(const struct command_line *line)
{
    auto jobs = split_jobs(line->exprs);
    int last_status = 0;
    bool should_execute = true;

    for (size_t i = 0; i < jobs.size(); ++i) {
        const auto& job = jobs[i];
        if (should_execute) {
            bool is_last_job = (i == jobs.size() - 1);
            last_status = execute_pipeline(job.exprs, line, is_last_job);
        }
        
        if (job.next_op == EXPR_TYPE_AND) {
            should_execute = (last_status == 0);
        } else if (job.next_op == EXPR_TYPE_OR) {
            should_execute = (last_status != 0);
        }
    }
    return last_status;
}

static int
execute_command_line(const struct command_line *line)
{
    if (line->exprs.empty())
        return 0;

    if (line->is_background) {
        pid_t pid = fork();
        if (pid == 0) {
            // Child
            int devnull = open("/dev/null", O_RDONLY);
            if (devnull != -1) {
                dup2(devnull, STDIN_FILENO);
                close(devnull);
            }
            throw ExitException(execute_jobs(line));
        } else if (pid > 0) {
            // Parent
            return 0;
        } else {
            perror("fork");
            return 1;
        }
    } else {
        return execute_jobs(line);
    }
}

int
main(void)
{
	const size_t buf_size = 1024;
	char buf[buf_size];
	int rc;
    int last_exit_code = 0;
    
    ParserGuard p;

    try {
        while ((rc = read(STDIN_FILENO, buf, buf_size)) > 0) {
            // Reap zombies
            int status;
            while (waitpid(-1, &status, WNOHANG) > 0) {
                // process reaped
            }

            parser_feed(p, buf, rc);
            
            while (true) {
                // Reap zombies
                int status;
                while (waitpid(-1, &status, WNOHANG) > 0) {
                    // process reaped
                }

                LineGuard line;
                enum parser_error err = parser_pop_next(p, line.ptr());
                if (err == PARSER_ERR_NONE && line.get() == NULL)
                    break;
                if (err != PARSER_ERR_NONE) {
                    printf("Error: %d\n", (int)err);
                    continue;
                }
                last_exit_code = execute_command_line(line.get());
            }
        }
    } catch (const ExitException& e) {
        return e.code;
    }

	return last_exit_code;
}
