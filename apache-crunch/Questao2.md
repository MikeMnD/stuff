Implemente o seguinte pipeline usando Apache Crunch (envie o código e o gráfico gerado pelo crunch mostrando o diagrama do pipeline (dot file))

* Lê um arquivo de logs que possui duas colunas. Na primeira coluna, os valores podem ser 1 ou 0
* Divida o log em duas partes: uma com as linhas começando com 0 e outra com as linhas começando com 1
* Faça um processador para contar quantas linhas começam com 0
* Faça um processador para contar as palavras da segunda coluna das linhas que começam com 1
* Grave o resultado em um arquivo texto