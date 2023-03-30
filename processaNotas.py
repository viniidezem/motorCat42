from db import insert_data, exec_sql
from preparaTabelas import montaTabelaSaida, montaTabelaEntrada, retornaNotasUtilizadasItem

notasUtilizadas = {}


def adicionaNotaUtilizada(autinc, qtdUti):
    if autinc in notasUtilizadas:
        notasUtilizadas[autinc] += qtdUti
    else:
        notasUtilizadas[autinc] = qtdUti


def consultaNotaUtilizada(autinc):
    return notasUtilizadas.get(autinc, 0)


def processaItems(item):
    # for item in itens:#
    codite = item['ITE_CODITE']
    codemp = item['ITE_CODEMP']
    resultCat42 = []
    notasUtilizadas = retornaNotasUtilizadasItem(codite, codemp)
    notasUtilizadas_Original = notasUtilizadas
    # print(codite, '  ', codemp)
    tabSaida = montaTabelaSaida(codite=codite, codemp=codemp)
    tabEntrada = montaTabelaEntrada(codite=codite, codemp=codemp)

    tabSaidaOrdenado = sorted(tabSaida, key=lambda x: x['DET_DTAENT'], reverse=False)
    tabEntradaOrdenado = sorted(tabEntrada, key=lambda x: x['DET_DTAENT'], reverse=False)

    for linhaSaida in tabSaidaOrdenado:

        # inicio processo entrada
        for linhaEntrada in tabEntradaOrdenado:

            vQtdNecessidadeSaida = linhaSaida["DET_QTDITE"]
            vQtdUtilizada = 0
            if linhaEntrada["DET_CODITE"] == linhaSaida["DET_CODITE"] and \
                    linhaEntrada["DET_DTAENT"] <= linhaSaida["DET_DTAENT"]:

                vCodite = linhaSaida["DET_CODITE"]
                vCodEmp = linhaSaida["DET_CODEMP"]
                vAutIncSaida = linhaSaida["DET_AUTINC"]
                vNumDocSaida = linhaSaida["DET_NUMDOC"]
                vEspDocSaida = linhaSaida["DET_ESPDOC"]
                vCodCliSaida = linhaSaida["DET_CODCLI"]
                vQtdIteSaida = linhaSaida["DET_QTDITE"]

                vAutIncEntrada = linhaEntrada["DET_AUTINC"]
                vNumDocEntrada = linhaEntrada["DET_NUMDOC"]
                vEspDocEntrada = linhaEntrada["DET_ESPDOC"]
                vCodCliEntrada = linhaEntrada["DET_CODFOR"]
                vQtdIteEntrada = linhaEntrada["DET_QTDITE"]

                vQtdEntradaDisponivel = vQtdIteEntrada - consultaNotaUtilizada(vAutIncEntrada)

                if vQtdNecessidadeSaida > vQtdEntradaDisponivel:
                    vQtdUtilizada = vQtdEntradaDisponivel
                elif vQtdEntradaDisponivel >= vQtdNecessidadeSaida:
                    vQtdUtilizada = vQtdNecessidadeSaida
                    vQtdNecessidadeSaida -= vQtdUtilizada

                if vQtdUtilizada > 0:
                    adicionaNotaUtilizada(vAutIncEntrada, vQtdUtilizada)
                    # print(vCodite, '-', vCodEmp, '-|Saida: ', vNumDocSaida, '-', vEspDocSaida, '-', vCodCliSaida, '-',
                    #       vQtdIteSaida, '|Entrada: ',
                    #       vAutIncEntrada, '-', vNumDocEntrada, '-', vEspDocEntrada, '-', vCodCliEntrada, '-',
                    #       vQtdUtilizada)
                    tupleCat42 = (vCodite, vCodEmp, vNumDocSaida, vEspDocSaida, vCodEmp, vCodCliSaida, vQtdIteSaida,
                                  vAutIncEntrada, vNumDocEntrada, vEspDocEntrada, vCodCliEntrada, vQtdUtilizada)
                    resultCat42.append(tupleCat42)

    sqlQry = "INSERT INTO CAT_42 " \
             "( CAT_CODITE , CAT_CODEMP , CAT_DOCSAI , CAT_ESPSAI , CAT_EMPSAI , CAT_CLISAI , " \
             "CAT_QTDSAI" \
             ", CAT_INCENT , CAT_DOCENT , CAT_ESPENT , CAT_CLIENT , CAT_QTDENT ) " \
             " VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"

    insert_data(sqlQry, tuple(resultCat42))

    sqlQry = "REPLACE INTO NFEUTI ( UTI_SEQDET, UTI_QTDUTI, UTI_CODITE ) VALUES ( %s, %s, '" + codite + "'  )"
    insert_data(sqlQry, tuple(notasUtilizadas.items()))

    sqlQry = "UPDATE ITENS_PROCESSAR_CAT SET REALIZ = 'S' WHERE ITE_CODITE = '" + codite + "' AND ITE_CODEMP = '" + codemp + "';"
    exec_sql(sqlQry, False, True)

# print(lista_ordenada)
# if len(tabSaida) > 0:
#     print(tabSaida[0]['DET_NUMDOC'])
#     print('a')
