from db import insert_data, exec_sql
from preparaTabelas import montaTabelaSaida, montaTabelaEntrada, retornaNotasUtilizadasItem
import time

def processaItems(item):
    tempo_inicial_item = time.time()
    notasUtilizadas = {}

    def adicionaNotaUtilizada(autinc, qtdUti):
        if autinc in notasUtilizadas:
            notasUtilizadas[autinc] += qtdUti
        else:
            notasUtilizadas[autinc] = qtdUti

    def consultaNotaUtilizada(autinc):
        return notasUtilizadas.get(autinc, 0)

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

    notasTotalmenteUtilizadas = []

    for linhaSaida in tabSaidaOrdenado:

        vQtdNecessidadeSaida = linhaSaida["DET_QTDITE"]

        for linhaEntradaTotalmenteUtilizada in reversed(notasTotalmenteUtilizadas):
            tabEntradaOrdenado.remove(linhaEntradaTotalmenteUtilizada)
            notasTotalmenteUtilizadas.remove(linhaEntradaTotalmenteUtilizada)
        # inicio processo entrada
        for linhaEntrada in tabEntradaOrdenado:
            if vQtdNecessidadeSaida != 0:
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
                        vQtdNecessidadeSaida -= vQtdUtilizada
                    elif vQtdEntradaDisponivel >= vQtdNecessidadeSaida:
                        vQtdUtilizada = vQtdNecessidadeSaida
                        vQtdNecessidadeSaida -= vQtdUtilizada

                    if vQtdUtilizada > 0:
                        adicionaNotaUtilizada(vAutIncEntrada, vQtdUtilizada)
                        tupleCat42 = (vCodite, vCodEmp,vAutIncSaida, vNumDocSaida, vEspDocSaida, vCodEmp, vCodCliSaida, vQtdIteSaida,
                                      vAutIncEntrada, vNumDocEntrada, vEspDocEntrada, vCodCliEntrada, vQtdUtilizada)
                        if len(tupleCat42) > 0:
                            resultCat42.append(tupleCat42)
                        if (vQtdEntradaDisponivel - vQtdUtilizada) == 0:
                            notasTotalmenteUtilizadas.append(linhaEntrada)

            else:
                break

    sqlQry = "INSERT INTO FIS_ENTSAI " \
             "( FIS_CODITE , FIS_CODEMP , FIS_INCSAI, FIS_DOCSAI , FIS_ESPSAI , FIS_EMPSAI , FIS_CLISAI , " \
             "FIS_QTDSAI" \
             ", FIS_INCENT , FIS_DOCENT , FIS_ESPENT , FIS_CLIENT , FIS_QTDENT ) " \
             " VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"

    insert_data(sqlQry, tuple(resultCat42))

    sqlQry = "REPLACE INTO FIS_NFEUTI ( UTI_SEQDET, UTI_QTDUTI, UTI_CODITE ) VALUES ( %s, %s, '" + codite + "'  )"
    insert_data(sqlQry, tuple(notasUtilizadas.items()))

    sqlQry = "UPDATE ITENS_PROCESSAR_FIS SET REALIZ = 'S' WHERE ITE_CODITE = '" + codite + "' AND ITE_CODEMP = '" + codemp + "';"
    exec_sql(sqlQry, False, True)

    tempo_final_item = time.time()
    tempo_execucao_item = tempo_final_item - tempo_inicial_item

    print("Tempo de execução:", tempo_execucao_item, "segundos do item:", codite , '-' , codemp)