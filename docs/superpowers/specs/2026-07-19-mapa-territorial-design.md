# Mapa Territorial Operacional

## Objetivo

Transformar o Mapa Territorial em uma tela operacional confiável para localizar
oportunidades periciais por município. O mapa deve representar os dados reais
coletados, evitar sobreposição de processos no mesmo ponto e continuar funcional
quando dependências externas falharem.

## Diagnóstico Atual

- O banco possui 729 processos.
- 702 processos têm município associado a uma geometria válida no PostGIS.
- Esses processos estão distribuídos em 73 municípios.
- O endpoint atual limita o resultado aos 120 maiores scores.
- Cada processo é desenhado como um marcador separado no centro do município.
- Processos do mesmo município ficam sobrepostos e parecem ser um único item.
- O frontend mantém uma lista manual de coordenadas para apenas 15 cidades.
- Leaflet e seus estilos são carregados em tempo de execução por CDN.
- Uma falha da CDN pode produzir tela branca ou ativar apenas o modo de fallback.

## Decisão De Produto

O mapa usará uma visualização híbrida:

1. Na visão estadual, cada município terá um marcador agregado.
2. O marcador mostrará a quantidade de processos do município.
3. A cor indicará a maior faixa de oportunidade pericial encontrada.
4. O clique selecionará o município e abrirá um painel lateral.
5. O painel listará os processos daquele município.
6. O usuário poderá abrir o detalhe de um processo e acompanhá-lo.
7. Não serão exibidos limites territoriais municipais ou estaduais.

## Layout

A página terá cabeçalho interno, barra compacta de filtros e uma área principal
dividida entre mapa e painel contextual.

### Cabeçalho

- Título: `Mapa Territorial`.
- Subtítulo com cobertura: processos georreferenciados e municípios representados.
- Ação de atualização.

### Filtros

- Região IMEA.
- Município.
- Faixa de probabilidade.
- Período de distribuição.
- Botão para limpar filtros.

Os filtros serão aplicados no backend. O contador deve diferenciar:

- total de processos encontrados;
- processos posicionados;
- municípios representados;
- processos sem localização.

### Mapa

- Ocupa a maior parte da largura disponível.
- Mantém dimensões estáveis em desktop e mobile.
- Usa basemap claro e discreto.
- Não desenha polígonos ou limites territoriais.
- Usa controles de zoom compactos.
- Exibe legenda das faixas de oportunidade.
- Marcadores municipais são círculos compactos, sem sombra.
- O número no marcador representa a quantidade de processos.
- Marcadores não mudam de tamanho ao carregar ou selecionar.

### Painel Lateral

Sem município selecionado:

- mostra orientação curta;
- apresenta os municípios com maior quantidade de processos;
- permite selecionar um município da lista.

Com município selecionado:

- nome do município;
- total de processos;
- maior score;
- quantidade por faixa;
- lista paginada, com 10 processos mais relevantes por página;
- CNJ;
- classe;
- data de distribuição;
- score;
- faixa;
- botão para abrir detalhes;
- botão para acompanhar processo, quando autorizado.

Em telas estreitas, o painel passa para baixo do mapa.

## Dados E API

O frontend não manterá coordenadas manuais.

Será criado um endpoint agregado:

`GET /api/processos/mapa/resumo`

Parâmetros:

- `regiao`;
- `municipio`;
- `faixa`;
- `data_inicio`;
- `data_fim`;
- `limit_cidades`.

Resposta:

```json
{
  "total_processos": 702,
  "total_municipios": 73,
  "sem_localizacao": 27,
  "items": [
    {
      "municipio": "Cuiabá",
      "regiao_imea": "Centro-Sul",
      "lat": -15.4156,
      "lng": -56.0517,
      "total_processos": 152,
      "maior_score": 27,
      "processos_quentes": 0,
      "processos_provaveis": 0,
      "faixa_dominante": "frio",
      "ultima_distribuicao": "2026-07-01"
    }
  ]
}
```

Será mantido o endpoint atual para compatibilidade.

O painel lateral consultará:

`GET /api/processos`

com os filtros atuais de município, região, faixa e datas.

## Integridade Dos Dados

- Coordenadas virão de `municipios_mt.geometry`.
- O ponto será calculado por `ST_PointOnSurface`.
- Somente municípios com geometria válida serão desenhados.
- A API informará separadamente processos sem localização.
- A contagem agregada será feita antes de qualquer limite.
- O limite será aplicado a municípios, não a processos individuais.
- Nenhuma coordenada será inferida de uma lista fixa no frontend.
- Dados inseridos em popups e painéis serão renderizados como texto, não como HTML
  concatenado sem escape.

## Dependências Cartográficas

- Leaflet será instalado via npm e incluído no bundle Vite.
- Tipos TypeScript do Leaflet serão instalados para evitar `any` desnecessário.
- Não haverá carregamento do código Leaflet via CDN.
- O endereço do provedor de tiles será configurável por variável de build.
- O desenvolvimento local usará OpenStreetMap com atribuição visível.
- Antes do lançamento comercial será obrigatório escolher um provedor com termos
  e disponibilidade adequados.

O agrupamento principal será realizado pelo backend por município. Isso evita a
necessidade de um plugin de clustering para a primeira versão. MarkerCluster
poderá ser adicionado posteriormente se o mapa passar a exibir pontos individuais.

## Estados E Falhas

### Carregando

- O contêiner do mapa mantém altura fixa.
- Um indicador discreto aparece sobre a área.

### API Indisponível

- Mensagem de erro com ação para tentar novamente.
- Nenhum contador antigo permanece como se fosse atual.

### Tiles Indisponíveis

- Marcadores e painel continuam disponíveis.
- A área do mapa mostra fundo neutro e aviso de basemap indisponível.
- A lista municipal continua navegável.

### Sem Resultados

- Mensagem específica para os filtros aplicados.
- Ação para limpar filtros.

### Município Sem Processos

- Painel volta ao estado inicial.

## Acessibilidade

- Controles possuem rótulos e foco visível.
- Marcadores têm texto alternativo contendo município e quantidade.
- Faixas não dependem apenas de cor; legenda e texto acompanham os indicadores.
- Painel lateral pode ser navegado por teclado.
- Contraste segue o sistema visual atual.

## Segurança

- Parâmetros continuam passando por SQLAlchemy com parâmetros nomeados.
- HTML externo não será injetado nos marcadores.
- Links e comandos respeitam as permissões atuais.
- Acompanhamento de processo continua usando o endpoint protegido existente.

## Testes

### Backend

- agregação por município;
- contagem total antes do limite;
- exclusão de municípios sem geometria;
- filtros por região, município, faixa e datas;
- contagem de processos sem localização;
- permissão de acesso.

### Frontend

- contrato do endpoint agregado;
- transformação de faixa em aparência do marcador;
- seleção de município;
- estado vazio;
- erro de API;
- ausência de tiles sem perda dos dados.

### Integração

- build Vite;
- suíte pytest;
- PostGIS real;
- smoke autenticado;
- confirmação de que 702 processos e 73 municípios são representados na base atual.

### Visual

- desktop largo;
- notebook;
- mobile;
- mapa não branco;
- marcadores visíveis;
- painel sem sobreposição;
- textos e botões sem cortes.

## Fora De Escopo

- limites territoriais;
- polígonos SIGEF, CAR, INPE ou assentamentos;
- geocodificação por serviços externos;
- mapa 3D;
- rotas;
- download offline de tiles;
- migração para MapLibre nesta fase.

## Critérios De Aceitação

- O mapa abre sem carregar Leaflet por CDN.
- A visão inicial representa todos os municípios com processos localizáveis.
- Municípios com vários processos não produzem pins sobrepostos.
- As contagens do mapa correspondem às consultas do PostGIS.
- O clique em município mostra processos reais daquele local.
- O usuário consegue abrir e acompanhar um processo pelo fluxo existente.
- Filtros atualizam mapa, contadores e painel.
- Falha de tiles não remove os dados operacionais.
- Nenhum limite territorial é exibido.
