package org.example

import groovy.xml.XmlSlurper
import java.sql.*
import java.net.URL
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class XmlDbUpdater {

    def xml
    String dbUrl, dbUser, dbPassword
    static final String COLUMN_VENDOR_CODE = "vendorCode"
    static final VALID_NAME_REGEX = /^[a-zA-Z_][a-zA-Z0-9_]*$/
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    XmlDbUpdater(String xmlUrl, String dbUrl, String dbUser, String dbPassword) {

        this.dbUrl = dbUrl
        this.dbUser = dbUser
        this.dbPassword = dbPassword


        def slurper = new XmlSlurper(false, false)
        try {
            // Отключаем загрузку внешних DTD, чтобы ускорить парсинг и избежать ошибок
            slurper.setFeature("http://apache.org/xml/features/disallow-doctype-decl", false)
            slurper.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false)
        } catch (Exception e) {
            println "Warning: XML parser features not set: $e"
        }

        xml = slurper.parse(new URL(xmlUrl).openStream())
    }

    void log(String message) {
        def timestamp = LocalDateTime.now().format(dtf)
        def line = "$timestamp - $message"
        println line
        logWriter?.println(line)
    }

    void validateName(String name) {
        if (name == null || !(name ==~ VALID_NAME_REGEX)) {
            throw new IllegalArgumentException("Invalid table or column name: " + name)
        }
    }

    ArrayList<String> getTableNames() {
        (xml.shop.children()
                .findAll { it.children().size() > 0 }
                *.name()
                .unique()) as ArrayList
    }

    ArrayList<String> getColumnNames(String tableName) {
        validate(tableName)
        def tableNode = xml.shop."$tableName"
        if (!tableNode || !tableNode.children()) return [] as ArrayList
        def cols = tableNode.children()[0].children()*.name()
        cols.each { validate(it) }
        cols as ArrayList
    }

    boolean isColumnId(String tableName, String columnName) {
        tableName.toLowerCase() == "offers" && columnName == COLUMN_VENDOR_CODE
    }

    String getTableDDL(String tableName) {
        validateName(tableName)

        // Формируем DDL для таблицы
        def columns = getColumnNames(tableName).collect { col ->
            // Проверяем имя колонки
            validate(col)

            // Формируем определение колонки
            def colDef = "\"$col\" TEXT"
            if (isColumnId(tableName, col)) {
                colDef += " PRIMARY KEY"
            }

            // Возвращаем определение для collect
            colDef
        }

// Собираем финальный SQL
        def ddl = "CREATE TABLE IF NOT EXISTS \"$tableName\" (${columns.join(', ')});"
        return ddl
    }

    String getDDLChange(String tableName) {
        validate(tableName)

        // Список уже существующих колонок в таблице
        def existing = []
        withConn { conn ->
            def rs = conn.metaData.getColumns(null, null, tableName, null)
            while (rs.next()) {
                existing << rs.getString("COLUMN_NAME")
            }
            rs.close()
        }

        // Список колонок из XML, которых ещё нет в таблице
        def newCols = getColumnNames(tableName).findAll { !(it in existing) }
        if (!newCols) return null

        // Формируем ALTER TABLE для каждой новой колонки
        newCols.collect { col ->
            "ALTER TABLE \"$tableName\" ADD COLUMN \"$col\" TEXT;"
        }.join("\n")
    }

    void ensureTable(String tableName) {
        withConn { it.createStatement().executeUpdate(getTableDDL(tableName)) }
    }

    void update(String tableName) {
        validate(tableName)
        ensureTable(tableName)

        def ddlChange = getDDLChange(tableName)
        if (ddlChange) {
            log("New columns detected for $tableName, consider applying DDL manually:\n$ddlChange")
        }

        def cols = getColumnNames(tableName)
        def idCol = cols.find { isColumnId(tableName, it) }
        def colList = cols.collect { "\"$it\"" }.join(", ")
        def placeholders = cols.collect { "?" }.join(", ")
        def updateSet = idCol ? cols.findAll { it != idCol }.collect { "\"$it\"=EXCLUDED.\"$it\"" }.join(", ") : null

        int inserted = 0, updated = 0
        withConn { conn ->
            xml.shop."$tableName".children().each { row ->
                def sql = idCol ?
                        "INSERT INTO \"$tableName\" ($colList) VALUES ($placeholders) ON CONFLICT(\"$idCol\") DO UPDATE SET $updateSet" :
                        "INSERT INTO \"$tableName\" ($colList) VALUES ($placeholders)"
                def ps = conn.prepareStatement(sql)
                cols.eachWithIndex { c, i -> ps.setString(i + 1, row."$c"?.text()) }
                int affected = ps.executeUpdate()
                if (idCol && affected == 1) inserted++ else if (idCol) updated++
                ps.close()
            }
        }
        log("Table $tableName updated: inserted=$inserted, updated=$updated")
    }

    void update() {
        getTableNames().each { update(it) }
    }

    void withConn(Closure c) {
        Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword)
        try { c(conn) } finally { conn.close() }
    }

    static void main(String[] args) {
        if (args.length < 4) {
            println "Usage: groovy XmlDbUpdater.groovy <xmlUrl> <dbUrl> <dbUser> <dbPassword>"
            System.exit(1)
        }

        def updater = new XmlDbUpdater(args[0], args[1], args[2], args[3])
        def scanner = new Scanner(System.in)

        while (true) {
            println "\n===== XML -> PostgreSQL Updater ====="
            println "1. Show tables"
            println "2. Show table DDL"
            println "3. Show table DDL changes"
            println "4. Update specific table"
            println "5. Update all tables"
            println "6. Exit"
            print "Enter choice: "

            switch (scanner.nextLine().trim()) {
                case "1":
                    println "Tables: ${updater.getTableNames()}"
                    break
                case "2":
                    while (true) {
                        print "Enter table name (or 'b' to go back): "
                        def tableName = scanner.nextLine().trim()
                        if (tableName.toLowerCase() == 'b') break
                        if (!updater.getTableNames().contains(tableName)) {
                            println "Error: Table '$tableName' not found in XML."
                        } else {
                            println updater.getTableDDL(tableName)
                            break
                        }
                    }
                    break
                case "3":
                    while (true) {
                        print "Enter table name (or 'b' to go back): "
                        def tableName = scanner.nextLine().trim()
                        if (tableName.toLowerCase() == 'b') break
                        if (!updater.getTableNames().contains(tableName)) {
                            println "Error: Table '$tableName' not found in XML."
                        } else {
                            println updater.getDDLChange(tableName) ?: "No new columns"
                            break
                        }
                    }
                    break
                case "4":
                    while (true) {
                        print "Enter table name (or 'b' to go back): "
                        def tableName = scanner.nextLine().trim()
                        if (tableName.toLowerCase() == 'b') break
                        if (!updater.getTableNames().contains(tableName)) {
                            println "Error: Table '$tableName' not found in XML."
                        } else {
                            updater.update(tableName)
                            break
                        }
                    }
                    break
                case "5":
                    updater.update()
                    break
                case "6":
                    System.exit(0)
                    break
                default:
                    println "Invalid choice"
            }
        }
    }
}
