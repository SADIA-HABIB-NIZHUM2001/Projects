#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

#define MAX_PROCESSES 64
#define MAX_CHILDREN  64
#define MAX_LINE      256

typedef enum {
    RUNNING    = 0,
    BLOCKED    = 1,
    ZOMBIE     = 2,
    TERMINATED = 3
} ProcessState;

static const char *STATE_NAMES[] = { "RUNNING", "BLOCKED", "ZOMBIE", "TERMINATED" };

typedef struct {
    int             pid;
    int             ppid;
    ProcessState    state;
    int             exit_status;
    int             children[MAX_CHILDREN];
    int             num_children;
    int             in_use;
    pthread_cond_t  child_exited;
    pthread_mutex_t wait_lock;
} PCB;

static PCB              table[MAX_PROCESSES];
static int              next_pid      = 2;
static pthread_mutex_t  table_lock    = PTHREAD_MUTEX_INITIALIZER;

static pthread_mutex_t  mon_lock      = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t   mon_cond      = PTHREAD_COND_INITIALIZER;
static int              table_updated = 0;
static int              all_done      = 0;

static FILE            *snap_file     = NULL;

static PCB *find_pcb(int pid)
{
    for (int i = 0; i < MAX_PROCESSES; i++)
        if (table[i].in_use && table[i].pid == pid)
            return &table[i];
    return NULL;
}

static PCB *alloc_slot(void)
{
    for (int i = 0; i < MAX_PROCESSES; i++)
        if (!table[i].in_use)
            return &table[i];
    return NULL;
}

static void write_snapshot(const char *header)
{
    if (!snap_file) return;
    fprintf(snap_file, "%s\n", header);
    fprintf(snap_file, "%-6s %-6s %-12s %s\n", "PID", "PPID", "STATE", "EXIT_STATUS");
    fprintf(snap_file, "----------------------------------------------\n");
    for (int i = 0; i < MAX_PROCESSES; i++) {
        PCB *p = &table[i];
        if (!p->in_use || p->state == TERMINATED) continue;
        char es[16];
        if (p->state == ZOMBIE)
            snprintf(es, sizeof(es), "%d", p->exit_status);
        else
            snprintf(es, sizeof(es), "-");
        fprintf(snap_file, "%-6d %-6d %-12s %s\n",
                p->pid, p->ppid, STATE_NAMES[p->state], es);
    }
    fprintf(snap_file, "\n");
    fflush(snap_file);
}

static void do_snapshot(const char *header)
{
    pthread_mutex_lock(&mon_lock);
    pthread_mutex_lock(&table_lock);
    write_snapshot(header);
    pthread_mutex_unlock(&table_lock);
    table_updated = 0;
    pthread_mutex_unlock(&mon_lock);
}

void pm_ps(void)
{
    pthread_mutex_lock(&table_lock);
    printf("%-6s %-6s %-12s %s\n", "PID", "PPID", "STATE", "EXIT_STATUS");
    printf("----------------------------------------------\n");
    for (int i = 0; i < MAX_PROCESSES; i++) {
        PCB *p = &table[i];
        if (!p->in_use || p->state == TERMINATED) continue;
        char es[16];
        if (p->state == ZOMBIE)
            snprintf(es, sizeof(es), "%d", p->exit_status);
        else
            snprintf(es, sizeof(es), "-");
        printf("%-6d %-6d %-12s %s\n",
               p->pid, p->ppid, STATE_NAMES[p->state], es);
    }
    printf("\n");
    pthread_mutex_unlock(&table_lock);
}

int pm_fork(int parent_pid, int thread_id)
{
    pthread_mutex_lock(&table_lock);

    PCB *parent = find_pcb(parent_pid);
    if (!parent || parent->state == ZOMBIE || parent->state == TERMINATED) {
        pthread_mutex_unlock(&table_lock);
        fprintf(stderr, "pm_fork: invalid parent %d\n", parent_pid);
        return -1;
    }

    PCB *child = alloc_slot();
    if (!child) {
        pthread_mutex_unlock(&table_lock);
        fprintf(stderr, "pm_fork: process table full\n");
        return -1;
    }

    int new_pid         = next_pid++;
    child->in_use       = 1;
    child->pid          = new_pid;
    child->ppid         = parent_pid;
    child->state        = RUNNING;
    child->exit_status  = -1;
    child->num_children = 0;
    pthread_cond_init(&child->child_exited, NULL);
    pthread_mutex_init(&child->wait_lock, NULL);

    if (parent->num_children < MAX_CHILDREN)
        parent->children[parent->num_children++] = new_pid;

    pthread_mutex_unlock(&table_lock);

    char hdr[256];
    snprintf(hdr, sizeof(hdr), "Thread %d calls pm_fork %d", thread_id, parent_pid);
    do_snapshot(hdr);

    return new_pid;
}

void pm_exit(int pid, int status, int thread_id)
{
    pthread_mutex_lock(&table_lock);

    PCB *p = find_pcb(pid);
    if (!p) {
        pthread_mutex_unlock(&table_lock);
        fprintf(stderr, "pm_exit: PID %d not found\n", pid);
        return;
    }

    p->state       = ZOMBIE;
    p->exit_status = status;

    PCB *parent = find_pcb(p->ppid);
    pthread_mutex_unlock(&table_lock);

    if (parent) {
        pthread_mutex_lock(&parent->wait_lock);
        pthread_cond_signal(&parent->child_exited);
        pthread_mutex_unlock(&parent->wait_lock);
    }

    char hdr[256];
    snprintf(hdr, sizeof(hdr), "Thread %d calls pm_exit %d %d", thread_id, pid, status);
    do_snapshot(hdr);
}

int pm_wait(int parent_pid, int child_pid, int thread_id)
{
    pthread_mutex_lock(&table_lock);

    PCB *parent = find_pcb(parent_pid);
    if (!parent || parent->num_children == 0) {
        pthread_mutex_unlock(&table_lock);
        return -1;
    }

    if (child_pid != -1) {
        int found = 0;
        for (int i = 0; i < parent->num_children; i++)
            if (parent->children[i] == child_pid) { found = 1; break; }
        if (!found) {
            pthread_mutex_unlock(&table_lock);
            fprintf(stderr, "pm_wait: %d is not a child of %d\n", child_pid, parent_pid);
            return -1;
        }
    }

    while (1) {
        for (int i = 0; i < parent->num_children; i++) {
            PCB *child = find_pcb(parent->children[i]);
            if (!child) continue;
            int match = (child_pid == -1) || (child->pid == child_pid);
            if (match && child->state == ZOMBIE) {
                int es        = child->exit_status;
                child->state  = TERMINATED;
                child->in_use = 0;
                parent->children[i] = parent->children[--parent->num_children];
                parent->state = RUNNING;
                pthread_mutex_unlock(&table_lock);

                char hdr[256];
                snprintf(hdr, sizeof(hdr), "Thread %d calls pm_wait %d %d",
                         thread_id, parent_pid, child_pid);
                do_snapshot(hdr);
                return es;
            }
        }

        parent->state = BLOCKED;
        pthread_mutex_unlock(&table_lock);

        pthread_mutex_lock(&parent->wait_lock);
        pthread_cond_wait(&parent->child_exited, &parent->wait_lock);
        pthread_mutex_unlock(&parent->wait_lock);

        pthread_mutex_lock(&table_lock);
        parent->state = RUNNING;
    }
}

void pm_kill(int pid, int thread_id)
{
    pthread_mutex_lock(&table_lock);

    PCB *p = find_pcb(pid);
    if (!p) {
        pthread_mutex_unlock(&table_lock);
        fprintf(stderr, "pm_kill: PID %d not found\n", pid);
        return;
    }

    p->state       = ZOMBIE;
    p->exit_status = 0;

    PCB *parent = find_pcb(p->ppid);
    pthread_mutex_unlock(&table_lock);

    if (parent) {
        pthread_mutex_lock(&parent->wait_lock);
        pthread_cond_signal(&parent->child_exited);
        pthread_mutex_unlock(&parent->wait_lock);
    }

    char hdr[256];
    snprintf(hdr, sizeof(hdr), "Thread %d calls pm_kill %d", thread_id, pid);
    do_snapshot(hdr);
}

void *monitor_thread(void *arg)
{
    (void)arg;
    pthread_mutex_lock(&table_lock);
    write_snapshot("Initial Process Table");
    pthread_mutex_unlock(&table_lock);

    pthread_mutex_lock(&mon_lock);
    while (1) {
        while (!table_updated && !all_done)
            pthread_cond_wait(&mon_cond, &mon_lock);
        if (!table_updated && all_done)
            break;
        table_updated = 0;
        pthread_mutex_unlock(&mon_lock);
        pthread_mutex_lock(&mon_lock);
    }
    pthread_mutex_unlock(&mon_lock);
    return NULL;
}

typedef struct {
    int  thread_id;
    char filename[256];
} WorkerArg;

void *worker_thread(void *arg)
{
    WorkerArg *wa = (WorkerArg *)arg;
    FILE *f = fopen(wa->filename, "r");
    if (!f) {
        fprintf(stderr, "Thread %d: cannot open %s\n", wa->thread_id, wa->filename);
        return NULL;
    }

    char line[MAX_LINE];
    while (fgets(line, sizeof(line), f)) {
        line[strcspn(line, "\r\n")] = '\0';
        if (strlen(line) == 0) continue;

        char cmd[32];
        if (sscanf(line, "%31s", cmd) != 1) continue;

        if (strcmp(cmd, "fork") == 0) {
            int ppid;
            if (sscanf(line, "%*s %d", &ppid) != 1) continue;
            int child = pm_fork(ppid, wa->thread_id);
            printf("[Thread %d] fork %d -> new PID %d\n", wa->thread_id, ppid, child);

        } else if (strcmp(cmd, "exit") == 0) {
            int pid, status;
            if (sscanf(line, "%*s %d %d", &pid, &status) != 2) continue;
            pm_exit(pid, status, wa->thread_id);
            printf("[Thread %d] exit %d %d\n", wa->thread_id, pid, status);

        } else if (strcmp(cmd, "wait") == 0) {
            int ppid, cpid;
            if (sscanf(line, "%*s %d %d", &ppid, &cpid) != 2) continue;
            int es = pm_wait(ppid, cpid, wa->thread_id);
            printf("[Thread %d] wait %d %d -> exit_status=%d\n",
                   wa->thread_id, ppid, cpid, es);

        } else if (strcmp(cmd, "kill") == 0) {
            int pid;
            if (sscanf(line, "%*s %d", &pid) != 1) continue;
            pm_kill(pid, wa->thread_id);
            printf("[Thread %d] kill %d\n", wa->thread_id, pid);

        } else if (strcmp(cmd, "sleep") == 0) {
            int ms;
            if (sscanf(line, "%*s %d", &ms) != 1) continue;
            usleep((useconds_t)ms * 1000);

        } else if (strcmp(cmd, "ps") == 0) {
            printf("[Thread %d] ps:\n", wa->thread_id);
            pm_ps();

        } else {
            fprintf(stderr, "[Thread %d] unknown command: %s\n", wa->thread_id, cmd);
        }
    }

    fclose(f);
    return NULL;
}

static void init_table(void)
{
    memset(table, 0, sizeof(table));
    for (int i = 0; i < MAX_PROCESSES; i++) {
        pthread_cond_init(&table[i].child_exited, NULL);
        pthread_mutex_init(&table[i].wait_lock, NULL);
    }
    table[0].in_use       = 1;
    table[0].pid          = 1;
    table[0].ppid         = 0;
    table[0].state        = RUNNING;
    table[0].exit_status  = -1;
    table[0].num_children = 0;
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        fprintf(stderr, "Usage: %s script0.txt script1.txt ...\n", argv[0]);
        return 1;
    }

    snap_file = fopen("snapshots.txt", "w");
    if (!snap_file) {
        fprintf(stderr, "Cannot open snapshots.txt\n");
        return 1;
    }

    init_table();

    pthread_t mon_tid;
    pthread_create(&mon_tid, NULL, monitor_thread, NULL);

    int        num_workers = argc - 1;
    pthread_t *tids = malloc(sizeof(pthread_t) * num_workers);
    WorkerArg *args = malloc(sizeof(WorkerArg)  * num_workers);

    for (int i = 0; i < num_workers; i++) {
        args[i].thread_id = i;
        strncpy(args[i].filename, argv[i + 1], sizeof(args[i].filename) - 1);
        pthread_create(&tids[i], NULL, worker_thread, &args[i]);
    }

    for (int i = 0; i < num_workers; i++)
        pthread_join(tids[i], NULL);

    pthread_mutex_lock(&mon_lock);
    all_done      = 1;
    table_updated = 0;
    pthread_cond_signal(&mon_cond);
    pthread_mutex_unlock(&mon_lock);

    pthread_join(mon_tid, NULL);

    printf("=== Final Process Table ===\n");
    pm_ps();

    fclose(snap_file);
    free(tids);
    free(args);
    return 0;
}
