import pyodbc


def dataSetting(excelData):
    cn = pyodbc.connect('DSN=Tibero6;UID=;PWD=')
    cursor = cn.cursor()

    tableName = excelData['TableName']
    schema = getSchema(cursor, tableName)
    select = "\n\t\t\tSELECT\n"
    data = {}
    dateList = list()

    if schema == "GMDMI":

        sql = getSql('getColumnList')
        cursor.execute(sql.format(schema, tableName))
        row = cursor.fetchone()

        rowPrefix = "\t\t\t\t  "
        firstRowCheck = True

        while row:
            column = row[0]

            if row[1] == "DATE":
                column = rowPrefix + "TO_CHAR(" + row[0] + ", \'YYYYMMDDHH24MISS\') AS " + row[0] + "\n"
                dateList.append(row[0])
            else:
                if row[0] == "INTF_TRNS_ID" or row[0] == "ROW_ID":
                    column = rowPrefix + row[0] + " AS ORGN_" + row[0] + "\n"
                else:
                    column = rowPrefix + row[0] + "\n"

            if firstRowCheck:
                rowPrefix = "\t\t\t\t, "
                firstRowCheck = False

            select = select + column

            row = cursor.fetchone()

        initialSelectQuery = select + "\t\t\tFROM " + schema + "." + tableName + "\n\t\t"
        selectQuery = initialSelectQuery + "\tWHERE INTF_CMPL_DTTM > TO_DATE(:#PREV_LAST_UPDATE_DATE, 'YYYYMMDDHH24MISS')\n\t\t"

        extract = {}
        extract['id'] = excelData['InterfaceId']
        extract['InterfaceId'] = excelData['InterfaceId']
        extract['KafkaTopic'] = "extract-history-" + excelData['InterfaceId']
        extract['InitialSelectQuery'] = initialSelectQuery
        extract['SelectQuery'] = selectQuery
        extract['CryptColumn'] = excelData['CryptColumn']

        collect = {}
        collect['id'] = "collect-" + excelData['InterfaceId']
        collect['InterfaceId'] = excelData['InterfaceId']
        collect['KafkaTopic'] = "extract-history-" + excelData['InterfaceId']
        collect['DateColumn'] = ','.join(dateList)
        collect['CollectTable'] = "DW" + schema + "." + tableName
        collect['CryptColumn'] = excelData['CryptColumn']

        data['sampleXml'] = "Extract_GMDMI_Sample.xml"
        data['extract'] = extract
        data['collect'] = collect

    elif schema == "GMDMO":

        sql = getSql('getColumnList')
        sql = sql + "\nAND COLUMN_NAME != 'DW_PRCS_YN'"
        cursor.execute(sql.format(schema, tableName))
        row = cursor.fetchone()

        rowPrefix = "\t\t\t\t  "
        firstRowCheck = True

        while row:
            column = row[0]

            if row[1] == "DATE":
                column = rowPrefix + "TO_CHAR(" + row[0] + ", \"YYYYMMDDHH24MISS\") AS " + row[0] + "\n"
                dateList.append(row[0])
            else:
                column = rowPrefix + row[0] + "\n"

            if firstRowCheck:
                rowPrefix = "\t\t\t\t, "
                firstRowCheck = False

            select = select + column

            row = cursor.fetchone()

        initialSelectQuery = select + "\t\t\tFROM " + schema + "." + tableName + "\n"
        initialSelectQuery = initialSelectQuery + "\t\t\tWHERE DW_PRCS_YN = 'O'\n\t\t"

        updateSuccessQuery = "\n\t\t\tUPDATE {0}.{1} \n\t\t\tSET DW_PRCS_YN = 'Y' \n\t\t\tWHERE DW_PRCS_YN = 'O'\n\t\t".format(schema, tableName)

        updatePrcsNtoOQuery = "\n\t\t\tUPDATE {0}.{1} \n\t\t\tSET DW_PRCS_YN = 'O' \n\t\t\tWHERE DW_PRCS_YN = 'N'\n\t\t".format(schema, tableName)

        extract = {}
        extract['id'] = excelData['InterfaceId']
        extract['InterfaceId'] = excelData['InterfaceId']
        extract['KafkaTopic'] = "extract-history-" + excelData['InterfaceId']
        extract['CryptColumn'] = excelData['CryptColumn']
        extract['InitialSelectQuery'] = initialSelectQuery
        extract['UpdateSuccessQuery'] = updateSuccessQuery
        extract['UpdatePrcsNtoOQuery'] = updatePrcsNtoOQuery


        collect = {}
        collect['id'] = "collect-" + excelData['InterfaceId']
        collect['InterfaceId'] = excelData['InterfaceId']
        collect['KafkaTopic'] = "extract-history-" + excelData['InterfaceId']
        collect['DateColumn'] = ','.join(dateList)
        collect['CollectTable'] = "DW" + schema + "." + tableName
        collect['CryptColumn'] = excelData['CryptColumn']

        data['sampleXml'] = "Extract_GMDMO_Sample.xml"
        data['extract'] = extract
        data['collect'] = collect

    elif "TOBE" in schema:
        sql = getSql('getColumnList')
        cursor.execute(sql.format(schema, tableName))
        row = cursor.fetchone()

        rowPrefix = "\t\t\t\t  "
        firstRowCheck = True

        while row:
            column = row[0]

            if row[1] == "DATE":
                column = rowPrefix + "TO_CHAR(" + row[0] + ", \'YYYYMMDDHH24MISS\') AS " + row[0] + "\n"
                dateList.append(row[0])
            else:
                if row[0] == "INTF_TRNS_ID" or row[0] == "ROW_ID":
                    column = rowPrefix + row[0] + " AS ORGN_" + row[0] + "\n"
                else:
                    column = rowPrefix + row[0] + "\n"

            if firstRowCheck:
                rowPrefix = "\t\t\t\t, "
                firstRowCheck = False

            select = select + column

            row = cursor.fetchone()

        initialSelectQuery = select + "\t\t\tFROM " + schema + "." + tableName + "\n\t\t"
        selectQuery = initialSelectQuery + "\tWHERE MODIFICATIONDTIME >"\
                                        +"\n\t\t\tTO_DATE(:#PREV_LAST_UPDATE_DATE, 'YYYYMMDDHH24MISS')\n\t\t"

        extract = {}
        extract['id'] = excelData['InterfaceId']
        extract['InterfaceId'] = excelData['InterfaceId']
        extract['KafkaTopic'] = "extract-history-" + excelData['InterfaceId']
        extract['CryptColumn'] = excelData['CryptColumn']
        extract['InitialSelectQuery'] = initialSelectQuery
        extract['SelectQuery'] = selectQuery


        collect = {}
        collect['id'] = "collect-" + excelData['InterfaceId']
        collect['InterfaceId'] = excelData['InterfaceId']
        collect['KafkaTopic'] = "extract-history-" + excelData['InterfaceId']
        collect['DateColumn'] = ','.join(dateList)
        collect['CollectTable'] = "DW" + schema + "." + tableName
        collect['CryptColumn'] = excelData['CryptColumn']

        data['sampleXml'] = "Extract_TOBE_Sample.xml"
        data['extract'] = extract
        data['collect'] = collect

    cn.commit()
    cn.close()

    return data

def getSql(fileName):
    f = open('sql/' + fileName + '.sql', 'r')
    sql = f.read()
    f.close()

    return sql

def getSchema(cursor, tableName):
    sql = getSql('getSchema')

    cursor.execute(sql.format(tableName))
    row = cursor.fetchone()

    return row[0]



