# Compliance de Fontes de Dados

Este documento organiza as fontes usadas ou previstas no Radar Pericial. Ele nao substitui revisao juridica. Antes de venda comercial, cada fonte deve ter licenca, termos de uso, politica de armazenamento e risco LGPD revisados.

## Matriz Inicial

| Fonte | Uso no produto | Armazenamento | Redistribuicao | Risco LGPD | Status antes de venda |
| --- | --- | --- | --- | --- | --- |
| DataJud/CNJ | Processos judiciais, score, alertas e inteligencia de presenca | Necessita revisao dos termos e limites | Nao redistribuir base bruta sem revisao | Medio/alto, pode conter dados processuais sensiveis | Revisao juridica obrigatoria |
| DOU | Eventos administrativos e oportunidades publicas | Geralmente publico, ainda exige revisao | Redistribuir apenas resumo/link quando possivel | Baixo/medio | Revisar licenca e fonte oficial |
| DNIT | Obras, desapropriacoes, servidao e infraestrutura | Revisar termos da fonte | Preferir metadados e link oficial | Baixo/medio | Revisar antes de producao |
| SINFRA-MT | Eventos de infraestrutura estadual | Revisar termos da fonte | Preferir metadados e link oficial | Baixo/medio | Revisar antes de producao |
| IBGE | Municipios, limites e metadados territoriais | Fonte publica amplamente usada | Confirmar atribuicao adequada | Baixo | Documentar atribuicao |
| SIGEF/INCRA | Imoveis rurais e dados fundiarios | Pode exigir cuidado com dados cadastrais | Evitar exposicao de dados pessoais | Medio/alto | Revisao obrigatoria antes de ativar em producao |
| INPE PRODES/DETER | Alertas ambientais e dados geoespaciais | Publico, com atribuicao | Confirmar termos e atribuicao | Baixo/medio | Documentar fonte e data |
| CAR/SICAR | Cadastro ambiental rural | Alto cuidado com dados cadastrais | Nao redistribuir dados brutos sem revisao | Alto | Manter desativado ate revisao juridica |

## Regras Operacionais

- Nao exibir dados pessoais desnecessarios.
- Nao salvar campos pessoais quando nao forem essenciais ao produto.
- Preferir armazenar metadados, links e identificadores publicos.
- Registrar fonte, data da coleta e data de atualizacao.
- Permitir rastreabilidade do dado ate a origem.
- Evitar redistribuicao de bases brutas.
- Separar uso interno de inteligencia de exposicao comercial.

## Pendencias Juridicas

Antes de lancamento comercial:

- revisar termos DataJud/CNJ;
- revisar limites de uso automatizado;
- revisar se a oferta comercial pode usar dados derivados;
- revisar LGPD para perfis profissionais e marketplace;
- criar termos de uso;
- criar politica de privacidade;
- criar aviso de responsabilidade sobre decisao pericial;
- definir canal para solicitacao de remocao/correcao de dados pessoais.

## Recomendacao de Produto

Para a primeira versao comercial, evitar vender acesso a dados brutos. O produto deve vender:

- inteligencia agregada;
- alertas;
- ranking;
- acompanhamento;
- perfil profissional;
- ferramentas de gestao.

Dados sensiveis ou potencialmente pessoais devem ser minimizados, protegidos ou removidos da interface.
