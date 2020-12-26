/* 
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not 
 * use this file except in compliance with the License. You may obtain a copy 
 * of the License at 
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0 
 *   
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT 
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the 
 * License for the specific language governing permissions and limitations 
 * under the License.
 * 
 */

package org.quartz.impl.jdbcjobstore;

import com.sun.xml.internal.bind.v2.TODO;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Internal database based lock handler for providing thread/resource locking 
 * in order to protect resources from being altered by multiple threads at the 
 * same time.
 * 
 * @author jhouse
 */
public class StdRowLockSemaphore extends DBSemaphore {

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * 
     * Constants.
     * 
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    public static final String SELECT_FOR_LOCK = "SELECT * FROM "
            + TABLE_PREFIX_SUBST + TABLE_LOCKS + " WHERE " + COL_SCHEDULER_NAME + " = " + SCHED_NAME_SUBST
            + " AND " + COL_LOCK_NAME + " = ? FOR UPDATE";

    public static final String INSERT_LOCK = "INSERT INTO "
        + TABLE_PREFIX_SUBST + TABLE_LOCKS + "(" + COL_SCHEDULER_NAME + ", " + COL_LOCK_NAME + ") VALUES (" 
        + SCHED_NAME_SUBST + ", ?)"; 

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * 
     * Constructors.
     * 
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */
    // 创建StdRowLockSemaphore数据库行锁处理器，同时调用父类DBSemaphore给数据库锁sql赋值
    public StdRowLockSemaphore() {
        super(DEFAULT_TABLE_PREFIX, null, SELECT_FOR_LOCK, INSERT_LOCK);
    }

    public StdRowLockSemaphore(String tablePrefix, String schedName, String selectWithLockSQL) {
        super(tablePrefix, schedName, selectWithLockSQL != null ? selectWithLockSQL : SELECT_FOR_LOCK, INSERT_LOCK);
    }

    // Data Members

    // Configurable lock retry parameters
    private int maxRetry = 3;
    private long retryPeriod = 1000L;

    public void setMaxRetry(int maxRetry) {
        this.maxRetry = maxRetry;
    }

    public void setRetryPeriod(long retryPeriod) {
        this.retryPeriod = retryPeriod;
    }

    public int getMaxRetry() {
        return maxRetry;
    }

    public long getRetryPeriod() {
        return retryPeriod;
    }

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     * 
     * Interface.
     * 
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    /**
     * Execute the SQL select for update that will lock the proper database row.
     */
    @Override
    protected void executeSQL(Connection conn, final String lockName, final String expandedSQL, final String expandedInsertSQL) throws LockException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        SQLException initCause = null;
        
        // attempt lock two times (to work-around possible race conditions in inserting the lock row the first time running)
        int count = 0;

        // Configurable lock retry attempts
        int maxRetryLocal = this.maxRetry;
        long retryPeriodLocal = this.retryPeriod;

        do {
            count++;
            try {
                // 准备prepareStatement
                ps = conn.prepareStatement(expandedSQL);
                ps.setString(1, lockName);
                
                if (getLog().isDebugEnabled()) {
                    getLog().debug(
                        "Lock '" + lockName + "' is being obtained: " + 
                        Thread.currentThread().getName());
                }
                // 执行select for update语句获得锁，如果此时该行锁被其他机器的线程获得，此时阻塞等待；
                // 若成功返回则有两种情况：【1】rs有记录则获得锁；【2】rs为空则说明还没有任何线程获得锁，此时插入一条街记录
                rs = ps.executeQuery();
                // 如果rs没有记录此时插入一条记录，TODO 问题：难道成功插入一条记录后就成功获得该锁吗？为啥呢
                if (!rs.next()) {
                    getLog().debug(
                            "Inserting new lock row for lock: '" + lockName + "' being obtained by thread: " + 
                            Thread.currentThread().getName());
                    rs.close();
                    rs = null;
                    ps.close();
                    ps = null;
                    ps = conn.prepareStatement(expandedInsertSQL);
                    ps.setString(1, lockName);
                    // 插入一条锁记录
                    int res = ps.executeUpdate();
                    // 如果没有成功插入一条记录，则重试while循环。说不定其他线程已经正在插入这个锁记录，然后重新执行for update获取锁
                    if(res != 1) {
                        if(count < maxRetryLocal) {
                            // pause a bit to give another thread some time to commit the insert of the new lock row
                            try {
                                Thread.sleep(retryPeriodLocal);
                            } catch (InterruptedException ignore) {
                                Thread.currentThread().interrupt();
                            }
                            // try again ...
                            continue;
                        }
                    
                        throw new SQLException(Util.rtp(
                            "No row exists, and one could not be inserted in table " + TABLE_PREFIX_SUBST + TABLE_LOCKS + 
                            " for lock named: " + lockName, getTablePrefix(), getSchedulerNameLiteral()));
                    }
                }
                // TODO 问题：成功插入一条记录后也会执行到这里，难道也代表获得了一把锁？
                return; // obtained lock, go
            } catch (SQLException sqle) {
                //Exception src =
                // (Exception)getThreadLocksObtainer().get(lockName);
                //if(src != null)
                //  src.printStackTrace();
                //else
                //  System.err.println("--- ***************** NO OBTAINER!");
    
                if(initCause == null)
                    initCause = sqle;
                
                if (getLog().isDebugEnabled()) {
                    getLog().debug(
                        "Lock '" + lockName + "' was not obtained by: " + 
                        Thread.currentThread().getName() + (count < maxRetryLocal ? " - will try again." : ""));
                }
                
                if(count < maxRetryLocal) {
                    // pause a bit to give another thread some time to commit the insert of the new lock row
                    try {
                        Thread.sleep(retryPeriodLocal);
                    } catch (InterruptedException ignore) {
                        Thread.currentThread().interrupt();
                    }
                    // try again ...
                    continue;
                }
                
                throw new LockException("Failure obtaining db row lock: "
                        + sqle.getMessage(), sqle);
            } finally {
                if (rs != null) { 
                    try {
                        rs.close();
                    } catch (Exception ignore) {
                    }
                }
                if (ps != null) {
                    try {
                        ps.close();
                    } catch (Exception ignore) {
                    }
                }
            }
        } while(count < (maxRetryLocal + 1));
        
        throw new LockException("Failure obtaining db row lock, reached maximum number of attempts. Initial exception (if any) attached as root cause.", initCause);
    }

    protected String getSelectWithLockSQL() {
        return getSQL();
    }

    public void setSelectWithLockSQL(String selectWithLockSQL) {
        setSQL(selectWithLockSQL);
    }
}
