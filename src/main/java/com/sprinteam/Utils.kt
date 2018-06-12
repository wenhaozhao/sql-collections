package com.sprinteam

import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.sql.Connection
import java.sql.SQLException

internal class StringUtils {
    companion object {
        fun isBlank(cs: CharSequence?): Boolean {
            cs ?: return true;
            if (cs.isEmpty()) {
                return true;
            }
            val strLen = cs.length;
            if (strLen != 0) {
                for (i in 0 until strLen) {
                    if (!Character.isWhitespace(cs[i])) {
                        return false
                    }
                }
                return true
            } else {
                return true
            }
        }

    }
}

internal class CollectionUtils {
    companion object {
        fun isEmpty(coll: Collection<Any>?): Boolean {
            return coll == null || coll.isEmpty()
        }

        fun isNotEmpty(coll: Collection<Any>?): Boolean {
            return !isEmpty(coll);
        }
    }
}

internal class MD5 {
    companion object {
        fun encode(text: String?): String {
            text ?: return ""
            try {
                val instance: MessageDigest = MessageDigest.getInstance("MD5")
                val digest:ByteArray = instance.digest(text.toByteArray())
                val sb = StringBuilder()
                for (b in digest) {
                    val i :Int = b.toInt() and 0xff
                    val hexString = Integer.toHexString(i)
                    if (hexString.length < 2) {
                        sb.append("0")
                    }
                    sb.append(hexString)
                }
                return sb.toString()
            } catch (e: NoSuchAlgorithmException) {
                e.printStackTrace()
                return ""
            }
        }
    }
}

internal class TxUtil {
    companion object {

        fun <T> retryOnException(exec: () -> T) = retryOnException(exec){c,_ -> c < 10 }

        /**
         * 异常重试
         */
        fun <T> retryOnException(exec: () -> T, retry: (Int,Exception?) -> Boolean) : T {
            var exception: Exception? = null
            var count = 0
            while (retry(count++, exception)) {
                try {
                    return exec()
                } catch (e: Exception) {
                    exception = e
                    Thread.sleep(10L)
                }
            }
            throw exception ?: Exception("Retry failed")
        }

        fun <T> tx(txProvider: Connection, inTx:(TxProvider)-> T) : T {
            val closeable = txProvider !is TxProvider
            val conn = if (txProvider is TxProvider) txProvider.proxy else txProvider
            try {
                conn.autoCommit = false
                try{
                    val _txProvider = if (txProvider is TxProvider) txProvider else TxProvider(conn)
                    return inTx.invoke(_txProvider)
                }finally {
                    if (closeable)
                        conn.commit()
                }
            } catch (e: SQLException) {

                try{
                    if (closeable) {
                        conn.rollback()
                    }
                }finally {
                    throw e
                }

            } finally {
                if (closeable) {
                    conn.autoCommit = true
                    conn.close()
                }
            }
        }
    }
}

internal class StringFix {
    companion object {
        fun escape(str: String): String{
            return str.replace("'","''").replace("\\","\\\\")
        }
    }
}