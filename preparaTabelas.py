class PreparaTabelas:
    def __init__(self, db):
        self.db = db

    def itensProcessar(self):
        cmd = "SELECT ITE_CODITE, ITE_CODEMP FROM ITENS_PROCESSAR_CAT WHERE REALIZ = 'N'"
        result = self.db.execute_query(cmd, True)
        return result

    def montaTabelaSaida(self, codite, codemp):
        cmd = ("SELECT * FROM ( "
               "SELECT 'MOVDET' TABELA, MOVDET.AUTOINCREM DET_AUTINC, DET_NUMDOC, DET_CODEMP, DET_ESPDOC, DET_CODITE, "
               "DET_CODCLI, DET_CODFOR, DET_TIP_FC, DET_CLAFIS, DET_QTDITE, DET_DTAENT, DET_HORENT "
               "FROM MOVDET INNER JOIN ITEGER ON ITE_CODITE = DET_CODITE AND ITE_CODEMP = DET_CODEMP "
               "WHERE DET_DTAENT BETWEEN ITE_DTABAL AND SUBDATE(CURDATE(), INTERVAL 1 DAY) "
               "  AND DET_CLAFIS > 5000 "
               "  AND DET_ESPDOC IN ('NF','IF') "
               "  AND DET_CODITE = '" + codite + "' "
               "  AND DET_CODEMP = '" + codemp + "' "                     
               "  UNION ALL "
               "SELECT 'NOTDET' TABELA, NOTDET.AUTOINCREM DET_AUTINC, DET_NUMDOC, DET_CODEMP, DET_ESPDOC, DET_CODITE, "
               "DET_CODCLI, DET_CODFOR, DET_TIP_FC, DET_CLAFIS, DET_QTDITE, DET_DTAENT, DET_HORENT "
               "FROM NOTDET INNER JOIN ITEGER ON ITE_CODITE = DET_CODITE AND ITE_CODEMP = DET_CODEMP "
               "WHERE DET_DTAENT BETWEEN ITE_DTABAL AND SUBDATE(CURDATE(), INTERVAL 1 DAY) "
               "  AND DET_CLAFIS > 5000 "
               "  AND DET_ESPDOC IN ('NF','IF') "
               "  AND DET_CODITE = '" + codite + "' "
               "  AND DET_CODEMP = '" + codemp + "' ) T "
               "ORDER BY DET_DTAENT ASC, DET_HORENT ASC; ")
        result = self.db.execute_query(cmd, True)
        return result

    def montaTabelaEntrada(self, codite, codemp):
        cmd = ("SELECT * FROM ( "
               "SELECT 'MOVDET' TABELA, MOVDET.AUTOINCREM DET_AUTINC, DET_NUMDOC, DET_CODEMP, DET_ESPDOC, DET_CODITE, "
               "DET_CODCLI, DET_CODFOR, DET_TIP_FC, DET_CLAFIS, DET_QTDITE, DET_DTAENT, DET_HORENT "
               "FROM MOVDET INNER JOIN ITEGER ON ITE_CODITE = DET_CODITE AND ITE_CODEMP = DET_CODEMP "
               "WHERE DET_DTAENT BETWEEN ITE_DTABAL AND SUBDATE(CURDATE(), INTERVAL 1 DAY) "
               "  AND DET_CLAFIS < 5000 "
               "  AND DET_ESPDOC IN ('NF','IF') "
               "  AND DET_CODITE = '" + codite + "'"
               "  AND DET_CODEMP = '" + codemp + "'"                     
               "  UNION ALL "
               "SELECT 'NOTDET' TABELA, NOTDET.AUTOINCREM DET_AUTINC, DET_NUMDOC, DET_CODEMP, DET_ESPDOC, DET_CODITE, "
               "DET_CODCLI, DET_CODFOR, DET_TIP_FC, DET_CLAFIS, DET_QTDITE, DET_DTAENT, DET_HORENT "
               "FROM NOTDET INNER JOIN ITEGER ON ITE_CODITE = DET_CODITE AND ITE_CODEMP = DET_CODEMP "
               "WHERE DET_DTAENT BETWEEN ITE_DTABAL AND SUBDATE(CURDATE(), INTERVAL 1 DAY) "
               "  AND DET_CLAFIS < 5000 "
               "  AND DET_ESPDOC IN ('NF','IF') "
               "  AND DET_CODITE = '"+ codite +"' "
               "  AND DET_CODEMP = '"+ codemp +"' ) T "
               "ORDER BY DET_DTAENT ASC, DET_HORENT ASC; ")
        result = self.db.execute_query(cmd, True)
        return result

    def retornaNotasUtilizadasItem(self, codite, codemp):
        cmd = ("SELECT UTI_SEQDET, UTI_QTDUTI FROM NFEUTI WHERE UTI_CODITE = '"+codite+"';")
        lista = self.db.execute_query(cmd, True)
        result = {}
        for dict in lista:
            chave = dict['UTI_SEQDET']
            valor = dict['UTI_QTDUTI']
            result[chave] = valor
        return result
