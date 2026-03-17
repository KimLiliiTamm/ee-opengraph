# POLÍTICA DE PRIVACIDADE — World Transparency Graph (WTG)

Policy-Version: v1.0.0  
Effective-Date: 2026-02-28  
Owner: WTG Governance Team

## Telemetria e logs coletados

A WTG pode coletar telemetria operacional para confiabilidade e prevenção de abuso:

- Metadados de requisição (timestamp, rota, status, latência).
- Eventos de rate limiting e sinais de segurança.
- Erros da plataforma e traces de diagnóstico.

## O que não é coletado/exposto

No modo público seguro, a WTG não expõe intencionalmente resultados com entidades de pessoa física ou identificadores pessoais.

A plataforma não deve ser usada como sistema de acusação criminal automática.

## Retenção e controle de acesso

- Logs são retidos por janela operacional limitada, conforme segurança e incidentes.
- Acesso a logs operacionais é restrito a mantenedores autorizados.
- Detalhes sensíveis de infraestrutura ficam fora da superfície pública.

## Logs para investigação de abuso

Quando houver suspeita de abuso, a WTG pode preservar logs relevantes para análise e aplicação de políticas.  
A resposta segue [ABUSE_RESPONSE.md](ABUSE_RESPONSE.md) e [TERMS.md](TERMS.md).

Para direitos e correções, veja [LGPD.md](LGPD.md).
