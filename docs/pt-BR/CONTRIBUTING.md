# Contribuindo com o BR/ACC Open Graph

Idioma: [English](../../CONTRIBUTING.md) | **Português (Brasil)**

Obrigado por contribuir com o BR/ACC Open Graph.

## Regras Gerais

- Mantenha as mudanças alinhadas ao objetivo de transparência de interesse público.
- Não adicione segredos, credenciais ou detalhes de infraestrutura privada.
- Respeite defaults públicos de segurança, privacidade e compliance.

## Setup de Desenvolvimento

```bash
cd api && uv sync --dev
cd ../etl && uv sync --dev
cd ../frontend && npm install
```

## Checagens de Qualidade

Execute antes de abrir PR:

```bash
make check
make neutrality
```

## Expectativas para Pull Request

- Mantenha o escopo da PR focado e explique o impacto para usuário.
- Inclua testes para mudanças de comportamento.
- Atualize documentação quando interfaces ou fluxos mudarem.
- Garanta todos os checks obrigatórios verdes no CI.

## Contribuições com Assistência de IA

Contribuições com assistência de IA são permitidas.  
Contribuidores humanos continuam responsáveis por:

- correção técnica,
- conformidade de segurança e privacidade,
- revisão final e aprovação antes do merge.
