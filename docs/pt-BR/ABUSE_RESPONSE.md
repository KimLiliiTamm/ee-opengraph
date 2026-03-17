# PLAYBOOK DE RESPOSTA A ABUSO — World Transparency Graph (WTG)

Policy-Version: v1.0.0  
Effective-Date: 2026-02-28  
Owner: WTG Governance Team

## Guia operacional não contratual

Este playbook é orientação operacional interna.  
Não cria dever contratual, resultado garantido ou direito de terceiro.

## Matriz de severidade

- Severidade 1 (Crítica): abuso ativo com alto potencial de dano (doxxing/extorsão/abuso automatizado em escala).
- Severidade 2 (Alta): violações repetidas ou uso direcionado indevido.
- Severidade 3 (Média): violações pontuais de baixo impacto imediato.
- Severidade 4 (Baixa): comportamento suspeito para monitoramento.

## Triagem e resposta

Passos mínimos:

1. Capturar metadados e timestamps do incidente.
2. Classificar severidade.
3. Preservar logs e evidências relevantes.
4. Aplicar controles proporcionais (rate-limit, bloqueio temporário, suspensão).

Controles possíveis:

- limitação de requisições;
- rotação de chaves/tokens quando aplicável;
- restrição de endpoints ou acesso;
- nota formal de incidente em logs de governança.

Todos os controles são aplicados em melhor esforço, de forma proporcional ao risco e às restrições legais.

## Escalonamento e retenção de evidência

Fluxo de escalonamento:

1. triagem de mantenedor;
2. revisão de governança/jurídico para casos de alto impacto;
3. escalonamento externo apenas quando exigido por lei.

Retenção de evidências:

- manter pelo período mínimo necessário para resposta, obrigação legal e auditabilidade;
- restringir acesso da evidência a mantenedores autorizados.

Políticas relacionadas:

- [ETHICS.md](ETHICS.md)
- [TERMS.md](TERMS.md)
- [PRIVACY.md](PRIVACY.md)
