package com.sprinteam

import java.sql.Connection
import java.util.*

/**
 * Author zhaowenhao
 * Date 27/03/2018 14:13
 * 基于RDBMS(MySQL)的字符串存储队列
 */
class SQLQueue(conn: ()-> Connection, val qId: String = UUID.randomUUID().toString()) : Queue<String?> {

    private val dao = Dao(conn, qId)

    private class Dao(val conn: () -> Connection, private val qId: String) {

        private val tableName = "t_queue_${this.qId}"

        init {
            //查找或者创建Table
            conn().use { conn ->
                conn.createStatement().use { smt ->
                    smt.execute("CREATE TABLE IF NOT EXISTS `${this.tableName}` (" +
                            "  `id` int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY," +
                            "  `md5` varchar(255) NOT NULL," +
                            "  `content` MEDIUMTEXT," +
                            "  `ts` bigint(20) NOT NULL," +
                            "  KEY `idx_t_md5` (`md5`)," +
                            "  KEY `idx_t_ts` (`ts`)" +
                            ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin")
                }
            }
        }

        fun drop(){
            conn().use{ it.createStatement().execute("DROP TABLE `${tableName}`")}
        }

        fun contains(entity: StringEntity, _conn: Connection? = null, next: ((TxProvider, peek: Boolean) -> Boolean)? = null): Boolean {
            return TxUtil.tx(_conn ?: conn()) { txProvider ->
                txProvider.createStatement().use {
                    it.executeQuery("SELECT COUNT(1) FROM `${tableName}` AS `T` WHERE `T`.`MD5`='${entity.md5}'").use {
                        val contains = it.next() && it.getLong(1) > 0L
                        next?.invoke(txProvider, contains) ?: contains
                    }
                }
            }
        }

        fun addAll(entities: Collection<StringEntity>, _conn: Connection? = null, next: ((TxProvider, peek: Boolean) -> Boolean)? = null): Boolean {
            return TxUtil.tx(_conn ?: conn()) { txProvider ->
                if (CollectionUtils.isEmpty(entities)){
                    next?.invoke(txProvider, false) ?: false
                }else {
                    val sql = "INSERT INTO `${tableName}` (`md5`,`content`,`ts`) VALUES ${entities.map { "('${it.md5}',${if (it.content == null) "null" else "'${StringFix.escape(it.content)}'"},${System.currentTimeMillis()})" }.reduce({ a, b -> "$a,$b" })}"
                    val inserted = txProvider.createStatement().use { TxUtil.retryOnException { it.executeUpdate(sql) > 0 } }
                    next?.invoke(txProvider, inserted) ?: inserted
                }
            }
        }

        fun clear(_conn: Connection? = null, next: ((TxProvider, peek: Boolean) -> Unit)? = null) {
            TxUtil.tx(_conn ?: conn()) { txProvider ->
                txProvider.createStatement().use {
                    val updated = TxUtil.retryOnException { it.execute("DELETE FROM `${tableName}`") }
                    next?.invoke(txProvider, updated)
                }
            }
        }

        fun elementAfter(id: Long = -1L, _conn: Connection? = null, next: ((TxProvider, peek: StringEntity?) -> StringEntity?)? = null): StringEntity? {
            return TxUtil.tx(_conn ?: conn()) { txProvider ->
                txProvider.createStatement().use {
                    it.executeQuery("SELECT `T`.`id`,`T`.`content` FROM `${tableName}` AS `T` WHERE `T`.`id`>${id} ORDER BY `T`.`ts` ASC LIMIT 0,1").use { rs ->
                        var selected: StringEntity? = null
                        if (rs.next()) {
                            selected = StringEntity(rs.getString("content"), rs.getLong("id"))
                        }
                        next?.invoke(txProvider, selected) ?: selected
                    }
                }
            }
        }

        fun elementBy(id: Long, _conn: Connection? = null, next: ((TxProvider, peek: StringEntity?) -> StringEntity?)? = null): StringEntity? {
            return TxUtil.tx(_conn ?: conn()) { txProvider ->
                txProvider.createStatement().use {
                    it.executeQuery("SELECT `T`.`id`,`T`.`content` FROM `${tableName}` AS `T` WHERE `T`.`id`=${id}").use { rs ->
                        var selected: StringEntity? = null
                        if (rs.next()) {
                            selected = StringEntity(rs.getString("content"), rs.getLong("id"))
                        }
                        next?.invoke(txProvider, selected) ?: selected
                    }
                }
            }
        }

        fun sizeAfter(id: Long = -1L, _conn: Connection? = null, next: ((TxProvider, peek: Int) -> Int)? = null): Int {
            return TxUtil.tx(_conn ?: conn()) { txProvider ->
                txProvider.createStatement().use {
                    it.executeQuery("SELECT COUNT(1) FROM `${tableName}` as `T` ${if (id >= 0L) "WHERE `T`.`id`>${id} ORDER BY `T`.`ts` ASC " else " "}").use { rs ->
                        var selected = 0
                        if (rs.next()) {
                            selected = rs.getInt(1)
                        }
                        next?.invoke(txProvider, selected) ?: selected
                    }
                }
            }

        }

        fun removeAfter(id: Long = -1L, _conn: Connection? = null, next: ((TxProvider, peek: StringEntity?) -> StringEntity?)? = null): StringEntity? {
            return TxUtil.tx(_conn ?: conn()) {
                elementAfter(id, it) { _txProvider, peek ->
                    TxUtil.tx(_txProvider) { txProvider ->
                        txProvider.createStatement().use {
                            TxUtil.retryOnException({
                                if (peek != null) {
                                    it.execute("DELETE FROM `${tableName}` WHERE `id`=${peek.id}")
                                }
                                next?.invoke(txProvider, peek) ?: peek
                            })
                        }
                    }
                }
            }
        }

        private fun allMatches(entities: Collection<StringEntity>,
                               md5In: Boolean = true,
                               _conn: Connection? = null,
                               next: ((TxProvider, peek: List<StringEntity>) -> List<StringEntity>)? = null): List<StringEntity> {
            return TxUtil.tx(_conn ?: conn()) { txProvider ->
                if (CollectionUtils.isEmpty(entities)) {
                    next?.invoke(txProvider, mutableListOf()) ?: mutableListOf()
                } else {
                    val sql = "SELECT `T`.`id`,`T`.`content` FROM `${tableName}` AS `T` WHERE `T`.`MD5` ${if (md5In) "IN " else "NOT IN "} (${entities.map { "'${it.md5}'" }.reduce { a, b -> "${a},${b}" }})"
                    txProvider.createStatement().use {
                        it.executeQuery(sql).use { rs ->
                            val matches = mutableListOf<StringEntity>()
                            while (rs.next()) {
                                matches.add(StringEntity(rs.getString("content"), rs.getLong("id")))
                            }
                            next?.invoke(txProvider, matches) ?: matches
                        }
                    }
                }
            }
        }

        private fun firstMatch(entity: StringEntity,
                               _conn: Connection? = null,
                               next: ((TxProvider, peek: StringEntity?) -> StringEntity?)? = null): StringEntity? {
            return TxUtil.tx(_conn ?: conn()) { txProvider ->
                txProvider.createStatement().use {
                    it.executeQuery("SELECT `T`.`id`,`T`.`content` FROM `${tableName}` AS `T` WHERE `T`.`MD5`='${entity.md5}' ORDER BY `T`.`ts` ASC LIMIT 0,1").use { rs ->
                        var match: StringEntity? = null
                        if (rs.next()) {
                            match = StringEntity(rs.getString("content"), rs.getLong("id"))
                        }
                        next?.invoke(txProvider, match) ?: match
                    }
                }
            }
        }

        private fun <T> removeBy(ids: Collection<Long>, lastPeek: T, _conn: Connection? = null, next: ((TxProvider, peek: T) -> T)? = null): T {
            return TxUtil.tx(_conn ?: conn()) {
                if (CollectionUtils.isEmpty(ids)) {
                    next?.invoke(it, lastPeek) ?: lastPeek
                } else {
                    TxUtil.retryOnException {
                        it.createStatement().use { it.execute("DELETE `T` FROM `${tableName}` AS `T` WHERE `T`.`id` IN (${ids.map { "'${it}'" }.reduce { a, b -> "${a},${b}" }})") }
                    }
                    next?.invoke(it, lastPeek) ?: lastPeek
                }
            }
        }

        /**
         * @param entities entities
         * @param md5In true with md5 in(...), false with md5 not in(...)
         */
        fun removeAllMatches(entities: Collection<StringEntity>,
                             md5In: Boolean = true,
                             _conn: Connection? = null,
                             next: ((TxProvider, peek: List<StringEntity>) -> List<StringEntity>)? = null): List<StringEntity> {

            return TxUtil.tx(_conn ?: conn()) {
                if (CollectionUtils.isEmpty(entities)) {
                    next?.invoke(it, mutableListOf()) ?: mutableListOf()
                } else {
                    allMatches(entities, md5In, it) { txProvider, peek ->
                        if (CollectionUtils.isNotEmpty(peek)) {
                            removeBy(peek.map { it.id }, peek, txProvider) { _txProvider, _peek ->
                                next?.invoke(_txProvider, _peek) ?: _peek
                            }
                        } else {
                            next?.invoke(it, peek) ?: peek
                        }
                    }

                }

            }
        }

        fun removeFirstMatch(entity: StringEntity,
                             _conn: Connection? = null,
                             next: ((TxProvider, peek: StringEntity?) -> StringEntity?)? = null): StringEntity? {
            return TxUtil.tx(_conn ?: conn()) {
                firstMatch(entity, it) { txProvider, peek ->
                    if (peek == null) {
                        next?.invoke(txProvider, null)
                    } else {
                        removeBy(setOf(peek.id), peek, txProvider) { _txProvider, _peek ->
                            next?.invoke(_txProvider, _peek) ?: _peek
                        }
                    }
                }
            }
        }

        fun removeBy(id: Long, _conn: Connection? = null, next: ((TxProvider, peek: StringEntity?) -> StringEntity?)? = null): StringEntity? {
            return TxUtil.tx(_conn ?: conn()) {
                elementBy(id, it) { txProvider, peek ->
                    if (peek == null) {
                        next?.invoke(txProvider, null)
                    } else {
                        removeBy(setOf(peek.id), peek, txProvider) { _txProvider, _peek ->
                            next?.invoke(_txProvider, _peek) ?: _peek
                        }
                    }
                }
            }
        }

        fun containsAll(entities: Collection<StringEntity>, _conn: Connection? = null, next: ((TxProvider, peek: Boolean) -> Boolean)? = null): Boolean {

            return TxUtil.tx(_conn ?: conn()) {
                val containsAll = entities.stream().allMatch { e -> contains(e, it) }
                next?.invoke(it, containsAll) ?: containsAll
            }
        }

    }

    private data class StringEntity(val content: String?, val id: Long = -1L) {
        val md5
            get() = if (StringUtils.isBlank(content)) "" else MD5.encode(content)
    }

    override val size: Int
        get() = dao.sizeAfter()

    fun size(txProvider: TxProvider? = null, next: ((TxProvider, peek: Int) -> Unit)? = null): Int {
        return dao.sizeAfter(-1L, txProvider) { _txProvider, peek ->
            next?.invoke(_txProvider, peek)
            peek
        }
    }

    override fun contains(element: String?) = dao.contains(StringEntity(element))

    fun contains(element: String?, txProvider: TxProvider? = null, next: ((TxProvider, peek: Boolean) -> Unit)? = null): Boolean {
        return dao.contains(StringEntity(element), txProvider) { _txProvider, peek ->
            next?.invoke(_txProvider, peek)
            peek
        }
    }

    override fun addAll(elements: Collection<String?>) = dao.addAll(elements.map { e -> StringEntity(e) })

    fun addAll(elements: Collection<String?>, txProvider: TxProvider? = null, next: ((TxProvider, peek: Boolean) -> Unit)? = null): Boolean {
        return dao.addAll(elements.map { e -> StringEntity(e) }, txProvider) { _txProvider, peek ->
            next?.invoke(_txProvider, peek)
            peek
        }
    }

    override fun clear() = dao.clear()

    fun clear(txProvider: TxProvider? = null, next: ((TxProvider) -> Unit)? = null) {
        dao.clear(txProvider) { _txProvider, _ -> next?.invoke(_txProvider) }
    }

    override fun element(): String? = dao.elementAfter()?.content ?: throw NoSuchElementException()

    fun element(txProvider: TxProvider? = null, next: ((TxProvider, peek: String?) -> Unit)? = null): String? {
        return dao.elementAfter(-1L, txProvider) { _txProvider, peek ->
            if (peek == null) {
                throw NoSuchElementException()
            }
            next?.invoke(_txProvider, peek.content)
            peek
        }?.content
    }

    override fun isEmpty() = this.size <= 0

    fun isEmpty(txProvider: TxProvider? = null, next: ((TxProvider, peek: Int) -> Unit)? = null) = this.size(txProvider, next) <= 0

    override fun remove(): String? {
        val e = dao.removeAfter() ?: throw NoSuchElementException()
        return e.content
    }

    fun remove(txProvider: TxProvider? = null, next: ((TxProvider, peek: String?) -> Unit)? = null): String? {
        return dao.removeAfter(-1L, txProvider) { _txProvider, peek ->
            if (peek == null) {
                throw NoSuchElementException()
            }
            next?.invoke(_txProvider, peek.content)
            peek
        }?.content
    }

    override fun containsAll(elements: Collection<String?>) = dao.containsAll(elements.map { e -> StringEntity(e) })

    fun containsAll(elements: Collection<String?>, txProvider: TxProvider? = null, next: ((TxProvider, peek: Boolean) -> Unit)? = null): Boolean {
        return dao.containsAll(elements.map { e -> StringEntity(e) }, txProvider) { _txProvider, peek ->
            next?.invoke(_txProvider, peek)
            peek
        }
    }

    override fun iterator(): MutableIterator<String?> = SQLIterator(dao)
    private class SQLIterator(val dao: Dao) : MutableIterator<String?> {

        private var lastRet: StringEntity? = null
        private var current: StringEntity?
        private var currentElement: String?

        init {
            current = dao.elementAfter(-1L)
            currentElement = current?.content
        }

        override fun hasNext(): Boolean = current != null
        override fun next(): String? {
            if (current == null) {
                throw NoSuchElementException()
            }
            val x = currentElement
            lastRet = current
            current = dao.elementAfter(current!!.id)
            currentElement = current?.content
            return x
        }

        override fun remove() {
            if (lastRet == null) {
                throw IllegalStateException()
            }
            dao.removeBy(lastRet!!.id)
        }

    }

    fun iterator(onNext: (TxProvider, MutableIterator<String?>, nextVal: String?) -> Unit, txProvider: TxProvider? = null) {
        TxUtil.tx(txProvider ?: dao.conn()) {
            val iterator = TxSQLIterator(dao, it)
            while (iterator.hasNext()) {
                onNext(it, iterator, iterator.next())
            }
        }
    }

    private class TxSQLIterator(private val dao: Dao, val txProvider: TxProvider) : MutableIterator<String?> {

        private var lastRet: StringEntity? = null
        private var current: StringEntity?
        private var currentElement: String?

        init {
            current = dao.elementAfter(-1L, txProvider)
            currentElement = current?.content
        }

        override fun hasNext(): Boolean = current != null
        override fun next(): String? {
            if (current == null) {
                throw NoSuchElementException()
            }
            val x = currentElement
            lastRet = current
            current = dao.elementAfter(current!!.id, txProvider)
            currentElement = current?.content
            return x
        }

        override fun remove() {
            if (lastRet == null) {
                throw IllegalStateException()
            }
            dao.removeBy(lastRet!!.id, txProvider)
        }
    }

    override fun remove(element: String?) = dao.removeFirstMatch(StringEntity(element)) != null

    fun remove(element: String?, txProvider: TxProvider? = null, next: ((TxProvider, peek: String?) -> Unit)? = null): Boolean {
        return dao.removeFirstMatch(StringEntity(element), txProvider) { _txProvider, peek ->
            next?.invoke(_txProvider, peek?.content)
            peek
        } != null
    }

    override fun removeAll(elements: Collection<String?>) = dao.removeAllMatches(elements.map { e -> StringEntity(e) }).isNotEmpty()

    fun removeAll(elements: Collection<String?>,
                  txProvider: TxProvider? = null,
                  next: ((TxProvider, peek: Collection<String?>) -> Unit)? = null): Boolean {
        return dao.removeAllMatches(elements.map { e -> StringEntity(e) }, true, txProvider) { _txProvider, peek ->
            next?.invoke(_txProvider, peek.map { it.content })
            peek
        }.isNotEmpty()
    }

    override fun add(element: String?) = addAll(Collections.singleton(element))

    fun add(element: String?, txProvider: TxProvider? = null, next: ((TxProvider, peek: Boolean) -> Unit)? = null) = addAll(Collections.singleton(element), txProvider, next)

    override fun offer(e: String?): Boolean = addAll(Collections.singleton(e))

    fun offer(e: String?, txProvider: TxProvider? = null, next: ((TxProvider, peek: Boolean) -> Unit)? = null) = add(e, txProvider, next)

    override fun retainAll(elements: Collection<String?>) = dao.removeAllMatches(elements.map { e -> StringEntity(e) }, false).isNotEmpty()

    fun retainAll(elements: Collection<String?>, txProvider: TxProvider? = null, next: ((TxProvider, peek: Collection<String?>) -> Unit)? = null): Boolean {
        return dao.removeAllMatches(elements.map { e -> StringEntity(e) }, false, txProvider) { _txProvider, peek ->
            next?.invoke(_txProvider, peek.map { it.content })
            peek
        }.isNotEmpty()
    }

    override fun peek() = dao.elementAfter()?.content

    fun peek(txProvider: TxProvider? = null, next: ((TxProvider, peek: String?) -> Unit)? = null): String? {
        return dao.elementAfter(-1L, txProvider) { _txProvider, peek ->
            next?.invoke(_txProvider, peek?.content)
            peek
        }?.content
    }

    override fun poll() = dao.removeAfter()?.content

    fun poll(txProvider: TxProvider? = null, next: ((TxProvider, peek: String?) -> Unit)? = null): String? {
        return dao.removeAfter(-1L, txProvider) { _txProvider, peek ->
            next?.invoke(_txProvider, peek?.content)
            peek
        }?.content
    }

    fun drop() = dao.drop()

}