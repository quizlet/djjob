<?php

// This system is mostly a port of delayed_job: http://github.com/tobi/delayed_job

class DJException extends Exception { }

class DJRetryException extends DJException { }

abstract class DJTask {
    public function getMaxAttempts() {
        return 5;
    }

    public function getDelay() {
        return 5*60;
    }

    abstract public function perform();
}

class DJBase {
    
    private static $mail = null;
    private static $db = null;
    private static $outputLog = true;
    
    private static $dsn = "";
    private static $options = array(
      "mysql_user" => null,
      "mysql_pass" => null,
    );
    
    // use either `configure` or `setConnection`, depending on if 
    // you already have a PDO object you can re-use
    public static function configure($dsn, array $options = array()) {
        self::$dsn = $dsn;
        self::$options = array_merge(self::$options, $options);
    }

    public static function setConnection(PDO $db) {
        self::$db = $db;
    }

    // name of available mail class expects devSend(subject, message) static function
    public static function setMail($mail) {
        self::$mail = $mail;
    }

    public static function outputLog($outputLog) {
        self::$outputLog = $outputLog;
    }
    
    protected static function getConnection() {
        if (self::$db === null) {
            if (!self::$dsn) {
                throw new DJException("Please tell DJJob how to connect to your database by calling DJJob::configure(\$dsn, [\$options = array()]) or re-using an existing PDO connection by calling DJJob::setConnection(\$pdoObject). If you're using MySQL you'll need to pass the db credentials as separate 'mysql_user' and 'mysql_pass' options. This is a PDO limitation, see [http://stackoverflow.com/questions/237367/why-is-php-pdo-dsn-a-different-format-for-mysql-versus-postgresql] for an explanation.");
            }
            try {
                // http://stackoverflow.com/questions/237367/why-is-php-pdo-dsn-a-different-format-for-mysql-versus-postgresql
                if (self::$options["mysql_user"] !== null) {
                    self::$db = new PDO(self::$dsn, self::$options["mysql_user"], self::$options["mysql_pass"]);
                } else {
                    self::$db = new PDO(self::$dsn);
                }
                self::$db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
            } catch (PDOException $e) {
                throw new Exception("DJJob couldn't connect to the database. PDO said [{$e->getMessage()}]");
            }
        }
        return self::$db;
    }

    public static function sendError($subject, $message) {
        if (!self::$mail) {
            return;
        }
       forward_static_call(array(self::$mail, "devSend"), $subject, $message);
    }
    
    public static function runQuery($sql, array $params = array()) {
        $stmt = self::getConnection()->prepare($sql);
        $stmt->execute($params);
        
        $ret = array();
        if ($stmt->rowCount()) {
            // calling fetchAll on a result set with no rows throws a
            // "general error" exception
            foreach ($stmt->fetchAll(PDO::FETCH_ASSOC) as $r) $ret []= $r;
        }
        
        $stmt->closeCursor();
        return $ret;
    }
    
    public static function runUpdate($sql, array $params = array()) {
        $stmt = self::getConnection()->prepare($sql);
        $stmt->execute($params);
        return $stmt->rowCount();
    }
    
    protected static function log($mesg) {
        if (!self::$outputLog) return;
        echo "[".date('r')."] {$mesg}\n";
    }
}

class DJWorker extends DJBase {
    // This is a singleton-ish thing. It wouldn't really make sense to
    // instantiate more than one in a single request (or commandline task)
    
    public function __construct(array $options = array()) {
        $options = array_merge(array(
            "queue" => "default",
            "count" => 0,
            "sleep" => 5,
            "quit_after_empty" => false,
        ), $options);
        list($this->queue, $this->count, $this->sleep, $this->quit_after_empty) =
            array($options["queue"], $options["count"], $options["sleep"], $options["quit_after_empty"]);

        list($hostname, $pid) = array(trim(`hostname`), getmypid());
        $this->name = "host::$hostname pid::$pid";

        if (function_exists("pcntl_signal")) {
            pcntl_signal(SIGTERM, array($this, "handleSignal"));
            pcntl_signal(SIGINT, array($this, "handleSignal"));
        }
    }
    
    public function handleSignal($signo) {
        $signals = array(
            SIGTERM => "SIGTERM",
            SIGINT  => "SIGINT"
        );
        $signal = $signals[$signo];
        
        $this->log("* [WORKER] Received received {$signal}... Shutting down");
        $this->releaseLocks();
        die(0);
    }
    
    public function releaseLocks() {
        $this->runUpdate("
            UPDATE jobs
            SET locked_at = NULL, locked_by = NULL
            WHERE locked_by = ?",
            array($this->name)
        );
    }
    
    public function getNewJob() {
        // we can grab a locked job if we own the lock
        $rs = $this->runQuery("
            SELECT id
            FROM   jobs
            WHERE  queue = ?
            AND    (run_at IS NULL OR NOW() >= run_at)
            AND    (locked_at IS NULL OR locked_by = ?)
            AND    failed_at IS NULL
            ORDER BY RAND()
            LIMIT 1
        ", array($this->queue, $this->name));
        
        foreach ($rs as $r) {
            $job = new DJJob($this->name, $r["id"]);
            if ($job->acquireLock()) return $job;
        }
        
        return false;
    }
    
    public function start() {
        $this->log("* [JOB] Starting worker {$this->name} on queue::{$this->queue}");
        
        $count = 0;
        $job_count = 0;
        try {
            while ($this->count == 0 || $count < $this->count) {
                if (function_exists("pcntl_signal_dispatch")) pcntl_signal_dispatch();

                $count += 1;
                $job = $this->getNewJob($this->queue);

                if (!$job) {
                    $this->log("* [JOB] Failed to get a job, queue::{$this->queue} may be empty");
                    
                    sleep($this->sleep);
                    
                    if ($this->quit_after_empty) {
                        $this->log("* [JOB] Quitting worker {$this->name} on queue::{$this->queue} since queue was empty.");
                        die;
                    }
                    
                    continue;
                }

                $job_count += 1;
                $job->run();
            }
        } catch (Exception $e) {
            $this->log("* [JOB] unhandled exception::\"{$e->getMessage()}\"");
            self::sendError("DJWorker unhandled exception", $e->getMessage()."\n\n".var_export(debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS), true));
        }

        $this->log("* [JOB] Worker {$this->name} shutting down after running {$job_count} jobs, over {$count} polling iterations");
    }
}

class DJJob extends DJBase {

    private $handler = null;
    
    public function __construct($worker_name, $job_id, array $options = array()) {
        $this->worker_name = $worker_name;
        $this->job_id = (int) $job_id;
    }
    
    public function run() {
        # pull the handler from the db
        $handler = $this->getHandler();
        if (!($handler instanceof DJTask)) {
            $this->log("* [JOB] bad handler for job::{$this->job_id}, must implement DJTask");
            return false;
        }

        # run the handler
        try {

            $handler->perform();
            
            # cleanup
            $this->finish();
            return true;
            
        } catch (DJRetryException $e) {
            
            # signal that this job should be retried later
            $this->log("* [JOB] got DJRetryException ".$e->getMessage());
            $this->retryLater();
            return false;
            
        } catch (Exception $e) {
            
            $this->finishWithError($handler, $e);
            return false;
            
        }
    }
    
    public function acquireLock() {
        $this->log("* [JOB] attempting to acquire lock for job::{$this->job_id} on {$this->worker_name}");
        
        $lock = $this->runUpdate("
            UPDATE jobs
            SET    locked_at = NOW(), locked_by = ?
            WHERE  id = ? AND (locked_at IS NULL OR locked_by = ?) AND failed_at IS NULL
        ", array($this->worker_name, $this->job_id, $this->worker_name));

        if (!$lock) {
            $this->log("* [JOB] failed to acquire lock for job::{$this->job_id}");
            return false;
        }

        usleep(20000); // 20 ms

        $job = $this->runQuery("
            SELECT locked_by
            FROM jobs
            WHERE id = ? AND failed_at IS NULL
        ", array($this->job_id));

        if (!$job || $job[0]['locked_by'] !== $this->worker_name) {
            $this->log("* [JOB] failed to acquire lock for job::{$this->job_id}");
            return false;
        }
		
		$this->log("* [JOB] acquired lock for job::{$this->job_id} on {$this->worker_name}");
        
        return true;
    }
    
    public function releaseLock() {
        $this->runUpdate("
            UPDATE jobs
            SET locked_at = NULL, locked_by = NULL
            WHERE id = ?",
            array($this->job_id)
        );
    }
    
    public function finish() {
        $this->runUpdate(
            "DELETE FROM jobs WHERE id = ?", 
            array($this->job_id)
        );
        $this->log("* [JOB] completed job::{$this->job_id}");
    }
    
    public function finishWithError($handler, $e) {
        $error = $e->__toString()."\n";
        $this->runUpdate("
            UPDATE jobs
            SET attempts = attempts + 1,
                failed_at = IF(attempts >= ?, NOW(), NULL),
                error = IF(attempts >= ?, ?, NULL),
                run_at = DATE_ADD(NOW(), INTERVAL ? SECOND)
            WHERE id = ?",
            array(
                $handler->getMaxAttempts(),
                $handler->getMaxAttempts(),
                $error,
                $handler->getDelay(),
                $this->job_id
            )
        );
        $this->log("* [JOB] failure in job::{$this->job_id}: ".$error);
        self::sendError("DJJob failure in job::{$this->job_id}", $error."\n\n".var_export(debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS), true));
        $this->releaseLock();
    }
    
    public function retryLater() {
        $this->runUpdate("
            UPDATE jobs
            SET run_at = DATE_ADD(NOW(), INTERVAL ? SECOND),
                attempts = attempts + 1
            WHERE id = ?",
            array($this->getHandler()->getDelay(), $this->job_id)
        );
        $this->releaseLock();
    }
    
    /**
     * @return DJTask
     */
    public function getHandler() {
        if ($this->handler !== NULL) {
            return $this->handler;
        }
        $rs = $this->runQuery(
            "SELECT handler FROM jobs WHERE id = ?", 
            array($this->job_id)
        );
        if (!$rs)
            return false;
        $this->handler = unserialize($rs[0]["handler"]);
        return $this->handler;
    }
    
    public static function enqueue(DJTask $handler, $queue = "default", $run_at = null) {
        $affected = self::runUpdate(
            "INSERT INTO jobs (handler, queue, run_at, created_at) VALUES(?, ?, ?, NOW())",
            array(serialize($handler), (string) $queue, $run_at)
        );
        
        if ($affected < 1) {
            self::log("* [JOB] failed to enqueue new job");
            return false;
        }
        
        return true;
    }
    
    public static function bulkEnqueue(array $handlers, $queue = "default", $run_at = null) {
        
        $parameters = array();
        foreach ($handlers as $k => $handler) {

            if (!$handler) {
                self::log("* [JOB] skipping enqueueing of non-existant handler");
                unset($handlers[$k]);
                continue;
            }
            if (!($handler instanceof DJTask)) {
                self::log("* [JOB] skipping enqueueing of bad handler; must implement DJTask");
                unset($handlers[$k]);
                continue;
            }

            $parameters []= serialize($handler);
            $parameters []= (string) $queue;
            $parameters []= $run_at;
        }
        
        $sql = "INSERT INTO jobs (handler, queue, run_at, created_at) VALUES";
        $sql .= implode(",", array_fill(0, count($handlers), "(?, ?, ?, NOW())"));
    
        $affected = self::runUpdate($sql, $parameters);
                
        if ($affected < 1) {
            self::log("* [JOB] failed to enqueue new jobs");
            return false;
        }
        
        if ($affected != count($handlers))
            self::log("* [JOB] failed to enqueue some new jobs");
        
        return true;
    }
    
    public static function status($queue = "default") {
        $rs = self::runQuery("
            SELECT COUNT(*) as total, COUNT(failed_at) as failed, COUNT(locked_at) as locked
            FROM `jobs`
            WHERE queue = ?
        ", array($queue));
        $rs = $rs[0];
        
        $failed = $rs["failed"];
        $locked = $rs["locked"];
        $total  = $rs["total"];
        $outstanding = $total - $locked - $failed;
        
        return array(
            "outstanding" => $outstanding,
            "locked" => $locked,
            "failed" => $failed,
            "total"  => $total
        );
    }

}
