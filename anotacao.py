import pandas as pd

# carrega os dados das tabelas de venda e compra em dataframes do pandas
vendas = pd.read_csv("tabela_vendas.csv")
compras = pd.read_csv("tabela_compras.csv")

# cria um novo dataframe para armazenar os resultados da análise
resultados = pd.DataFrame(columns=["Num Doc Venda", "Num Doc Compra", "Cod Item", "Data Venda", "Data Compra", "Qtd"])

# itera pelas linhas da tabela de vendas
for index_venda, venda in vendas.iterrows():

    # filtra as compras com código de item e data correspondentes à venda
    compras_filtradas = compras[(compras["Cod Item"] == venda["Cod Item"]) &
                                (compras["Data"] <= venda["Data Venda"])]

    # itera pelas linhas da tabela de compras filtradas
    for index_compra, compra in compras_filtradas.iterrows():

        # verifica se a quantidade de compra é maior ou igual à quantidade de venda
        if compra["Qtd"] >= venda["Qtd"]:

            # adiciona uma nova linha ao dataframe de resultados
            resultados = resultados.append({
                "Num Doc Venda": venda["Num Doc"],
                "Num Doc Compra": compra["Num Doc"],
                "Cod Item": venda["Cod Item"],
                "Data Venda": venda["Data"],
                "Data Compra": compra["Data"],
                "Qtd": venda["Qtd"]
            }, ignore_index=True)

            # atualiza a quantidade de compra com o valor restante após a venda
            compras.loc[index_compra, "Qtd"] -= venda["Qtd"]

            # interrompe a iteração pelas compras filtradas
            break

# exibe o resultado da análise
print(resultados)
