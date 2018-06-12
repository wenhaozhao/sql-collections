package com.sprinteam

import java.sql.Connection
import java.util.*

/**
 * Author zhaowenhao
 * Date 28/03/2018 17:12
 * 基于RDBMS(MySQL)的字符串Map
 */
class SQLMap(conn: ()-> Connection, val mapId: String = UUID.randomUUID().toString()) : MutableMap<String,String?> {

    private val dao = Dao(conn, mapId)

    private class Dao(private val conn: () -> Connection, private val qId: String) {
        private val tableName = "t_map_${this.qId}"

        init {
            conn().use {
                it.createStatement().use {
                    it.execute("CREATE TABLE IF NOT EXISTS `${tableName}` (" +
                            "  `keyMd5` varchar(255) NOT NULL PRIMARY KEY," +
                            "  `key` varchar(255) NOT NULL ," +
                            "  `value` MEDIUMTEXT," +
                            "  `valueMd5` varchar(255) NOT NULL," +
                            "  `ts` bigint(20) NOT NULL," +
                            "  KEY `idx_t_key` (`key`)," +
                            "  KEY `idx_t_valueMd5` (`valueMd5`)," +
                            "  KEY `idx_t_ts` (`ts`)" +
                            ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin")
                }
            }
        }

        fun drop(){
            conn().use{ it.createStatement().execute("DROP TABLE `${tableName}`")}
        }

        fun all(_conn: Connection? = null, next: ((TxProvider, peek: Set<StringEntity>) -> Set<StringEntity>)? = null): Set<StringEntity> {
            return TxUtil.tx(_conn ?: conn()) { txProvider ->
                txProvider.createStatement().use {
                    it.executeQuery("SELECT `T`.`key`,`T`.`value` FROM `${tableName}` AS `T`").use {
                        val all = mutableSetOf<StringEntity>()
                        while (it.next()) {
                            all.add(StringEntity(it.getString("key"), it.getString("value")))
                        }
                        next?.invoke(txProvider, all) ?: all
                    }
                }
            }
        }

        fun size(_conn: Connection? = null, next: ((TxProvider, peek: Int) -> Int)? = null): Int {
            return TxUtil.tx(_conn ?: conn()) { txProvider ->
                txProvider.createStatement().use {
                    it.executeQuery("SELECT COUNT(1) FROM `${tableName}` as `T` ").use {
                        val size = if (it.next()) it.getInt(1) else 0
                        next?.invoke(txProvider, size) ?: size
                    }
                }
            }
        }

        fun getByKeys(entities: Collection<StringEntity>, _conn: Connection? = null, next: ((TxProvider, peek: List<StringEntity>) -> List<StringEntity>)? = null): List<StringEntity> {
            return TxUtil.tx(_conn ?: conn()) { txProvider ->
                if (entities.isEmpty()) {
                    next?.invoke(txProvider, mutableListOf()) ?: mutableListOf()
                } else {
                    txProvider.createStatement().use {
                        it.executeQuery("SELECT `T`.`key`,`T`.`value` FROM `${tableName}` AS `T` WHERE `T`.`keyMd5` in (${entities.map { "'${it.keyMd5}'" }.reduce { a, b -> "${a},${b}" }})").use {
                            val matches = mutableListOf<StringEntity>()
                            while (it.next()) {
                                matches.add(StringEntity(it.getString("key"), it.getString("value")))
                            }
                            next?.invoke(txProvider, matches) ?: matches
                        }
                    }
                }
            }
        }

        fun getByValues(entities: Collection<StringEntity>, _conn: Connection? = null, next: ((TxProvider, peek: List<StringEntity>) -> List<StringEntity>)? = null): List<StringEntity> {
            return TxUtil.tx(_conn ?: conn()) { txProvider ->
                if (entities.isEmpty()) {
                    next?.invoke(txProvider, mutableListOf()) ?: mutableListOf()
                } else {
                    txProvider.createStatement().use {
                        it.executeQuery("SELECT `T`.`key`,`T`.`value` FROM `${tableName}` AS `T` WHERE `T`.`valueMd5` in (${entities.map { "'${it.valueMd5}'" }.reduce { a, b -> "${a},${b}" }})").use {
                            val matches = mutableListOf<StringEntity>()
                            while (it.next()) {
                                matches.add(StringEntity(it.getString("key"), it.getString("value")))
                            }
                            next?.invoke(txProvider, matches) ?: matches
                        }
                    }
                }
            }
        }

        fun put(entity: StringEntity, _conn: Connection? = null, next: ((TxProvider, peek: StringEntity?) -> StringEntity?)? = null): StringEntity? {
            return TxUtil.tx(_conn ?: conn()) {
                getByKeys(setOf(entity), it) { txProvider, peek ->
                    val sql = if (peek.isEmpty()) {
                        "INSERT INTO `${tableName}`(`keyMd5`,`key`,`valueMd5`,`value`,`ts`) values ('${entity.keyMd5}','${entity.key}','${entity.valueMd5}',${if(entity.value == null) "null" else "'${StringFix.escape(entity.value!!)}'"},${System.currentTimeMillis()})"
                    } else {
                        "UPDATE `${tableName}` SET `valueMd5`='${entity.valueMd5}',`value`=${if(entity.value == null) "null" else "'${StringFix.escape(entity.value!!)}'"},`ts`=${System.currentTimeMillis()} WHERE `keyMd5`='${entity.keyMd5}'"
                    }
                    TxUtil.tx(txProvider) {
                        TxUtil.retryOnException {
                            it.createStatement().use {
                                it.execute(sql)
                                val nextVal = next?.invoke(txProvider, peek.firstOrNull())
                                if (nextVal == null) peek else mutableListOf(nextVal)
                            }
                        }
                    }
                }
            }.firstOrNull()
        }

        fun put(entities: Collection<StringEntity>, _conn: Connection? = null, next: ((TxProvider, peek: List<StringEntity>) -> List<StringEntity>)? = null): List<StringEntity> {
            return TxUtil.tx(_conn ?: conn()) { txProvider ->
                val previous = entities.map { entity -> put(entity, txProvider) }.filterNotNull()
                next?.invoke(txProvider, previous) ?: previous
            }
        }

        fun removeAllMatches(entities: Collection<StringEntity> = listOf(), _conn: Connection? = null, next: ((TxProvider, peek: List<StringEntity>) -> List<StringEntity>)? = null): List<StringEntity> {
            return TxUtil.tx(_conn ?: conn()) {
                val matches = if (entities.isEmpty()) {
                    this.all(it)
                } else {
                    getByKeys(entities, it)
                }
                if (matches.isNotEmpty()) {
                    val sql = "DELETE `T` FROM `${tableName}` AS `T` WHERE `T`.`keyMd5` IN (${matches.map { "'${it.keyMd5}'" }.reduce { a, b -> "${a},${b}" }})"
                    it.createStatement().use { it.execute(sql) }
                }
                next?.invoke(it, ArrayList(matches)) ?: ArrayList(matches)

            }
        }
    }

    private data class StringEntity(val key: String, var value: String? = null) {
        val keyMd5
            get() = if (StringUtils.isBlank(key)) "" else MD5.encode(key)

        val valueMd5
            get() = if (StringUtils.isBlank(value)) "" else MD5.encode(value)
    }

    private data class StringMutableEntry(override val key: String, override var value: String?, private val dao: Dao) : MutableMap.MutableEntry<String, String?> {

        override fun setValue(newValue: String?): String? {
            return dao.put(StringEntity(key, value), null) { _, peek ->
                this.value = newValue
                peek
            }?.value
        }
    }

    override val size: Int
        get() = dao.size()

    fun size(txProvider: TxProvider? = null, next: ((TxProvider, peek: Int) -> Unit)? = null): Int {
        return dao.size(txProvider) { _txProvider, peek ->
            next?.invoke(_txProvider, peek)
            peek
        }
    }

    override fun containsKey(key: String) = dao.getByKeys(listOf(StringEntity(key))).isNotEmpty()

    fun containsKey(key: String, txProvider: TxProvider? = null, next: ((TxProvider, peek: String?) -> Unit)? = null): Boolean {
        return dao.getByKeys(listOf(StringEntity(key)), txProvider) { _txProvider, peek ->
            next?.invoke(_txProvider, peek.firstOrNull()?.value)
            peek
        }.isNotEmpty()
    }

    override fun containsValue(value: String?) = dao.getByValues(listOf(StringEntity("", value))).isNotEmpty()

    fun containsValue(value: String?, txProvider: TxProvider? = null, next: ((TxProvider, peek: Map<String, String?>) -> Unit)? = null): Boolean {
        return dao.getByValues(listOf(StringEntity("", value)), txProvider) { _txProvider, peek ->
            next?.invoke(_txProvider, peek.associate { Pair(it.key, it.value) })
            peek
        }.isNotEmpty()
    }

    override fun get(key: String) = dao.getByKeys(listOf(StringEntity(key))).firstOrNull()?.value

    fun get(key: String, txProvider: TxProvider? = null, next: ((TxProvider, peek: String?) -> Unit)? = null): String? {
        return dao.getByKeys(listOf(StringEntity(key)), txProvider) { _txProvider, peek ->
            next?.invoke(_txProvider, peek.firstOrNull()?.value)
            peek
        }.firstOrNull()?.value
    }

    fun get(keys: Collection<String>, txProvider: TxProvider? = null, next: ((TxProvider, peek: Map<String, String?>) -> Unit)? = null): Map<String, String?> {
        return dao.getByKeys(keys.map { StringEntity(it) }, txProvider) { _txProvider, peek ->
            next?.invoke(_txProvider, peek.associate { Pair(it.key, it.value) })
            peek
        }.associate { Pair(it.key, it.value) }

    }

    override fun isEmpty() = this.size <= 0

    fun isEmpty(txProvider: TxProvider? = null, next: ((TxProvider, peek: Int) -> Unit)? = null) = this.size(txProvider, next) <= 0

    override val entries: MutableSet<MutableMap.MutableEntry<String, String?>>
        get() = dao.all().map { StringMutableEntry(it.key, it.value, dao) }.toCollection(HashSet())

    fun entries(txProvider: TxProvider? = null, next: ((TxProvider, peek: MutableSet<MutableMap.MutableEntry<String, String?>>) -> Unit)? = null): MutableSet<MutableMap.MutableEntry<String, String?>> {
        return dao.all(txProvider) { _txProvider, peek ->
            next?.invoke(_txProvider, peek.map { StringMutableEntry(it.key, it.value, dao) }.toCollection(HashSet()))
            peek
        }.map { StringMutableEntry(it.key, it.value, dao) }.toCollection(HashSet())
    }

    override val keys: MutableSet<String>
        get() = dao.all().map { it.key }.toCollection(HashSet())

    fun keys(txProvider: TxProvider? = null, next: ((TxProvider, peek: MutableSet<String>) -> Unit)? = null): MutableSet<String> {
        return dao.all(txProvider) { _txProvider, peek ->
            next?.invoke(_txProvider, peek.map { it.key }.toCollection(HashSet()))
            peek
        }.map { it.key }.toCollection(HashSet())
    }

    override val values: MutableCollection<String?>
        get() = dao.all().map { it.value }.toCollection(HashSet())

    fun values(txProvider: TxProvider? = null, next: ((TxProvider, peek: MutableCollection<String?>) -> Unit)? = null): MutableCollection<String?> {
        return dao.all(txProvider) { _txProvider, peek ->
            next?.invoke(_txProvider, peek.map { it.value }.toCollection(mutableListOf()))
            peek
        }.map { it.key }.toCollection(mutableListOf())
    }

    override fun clear() {
        dao.removeAllMatches()
    }

    fun clear(txProvider: TxProvider? = null, next: ((TxProvider, peek: Map<String, String?>) -> Unit)? = null) {
        dao.removeAllMatches(listOf(), txProvider) { _txProvider, peek ->
            next?.invoke(_txProvider, peek.associate { Pair(it.key, it.value) })
            peek
        }
    }

    override fun put(key: String, value: String?): String? {
        return dao.put(StringEntity(key, value))?.value
    }

    fun put(key: String, value: String?, txProvider: TxProvider? = null, next: ((TxProvider, peek: String?) -> Unit)? = null): String? {
        return dao.put(StringEntity(key, value), txProvider) { _txProvider, peek ->
            next?.invoke(_txProvider, peek?.value)
            peek
        }?.value
    }

    override fun putAll(from: Map<out String, String?>) {
        dao.put(from.map { StringEntity(it.key, it.value) })
    }

    fun putAll(from: Map<out String, String?>, txProvider: TxProvider? = null, next: ((TxProvider, peek: Map<String, String?>) -> Unit)? = null) {
        dao.put(from.map { StringEntity(it.key, it.value) }, txProvider) { _txProvider, peek ->
            next?.invoke(_txProvider, peek.associate { Pair(it.key, it.value) })
            peek
        }
    }

    override fun remove(key: String) = dao.removeAllMatches(listOf(StringEntity(key))).firstOrNull()?.value

    fun remove(key: String, txProvider: TxProvider? = null, next: ((TxProvider, peek: String?) -> Unit)? = null): String? {
        return dao.removeAllMatches(listOf(StringEntity(key)), txProvider) { _txProvider, peek ->
            next?.invoke(_txProvider, peek.firstOrNull()?.value)
            peek
        }.firstOrNull()?.value
    }

    fun removeAllMatches(keys: Collection<String>, txProvider: TxProvider? = null, next: ((TxProvider, peek: Map<String, String?>) -> Unit)? = null): Map<String, String?> {
        return dao.removeAllMatches(keys.map { StringEntity(it) }, txProvider) { _txProvider, peek ->
            next?.invoke(_txProvider, peek.associate { Pair(it.key, it.value) })
            peek
        }.associate { Pair(it.key, it.value) }
    }

    fun drop() = dao.drop()
}