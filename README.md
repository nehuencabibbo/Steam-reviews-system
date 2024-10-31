# Steam-reviews-system
Trabajo práctico de la materia sistemas distribuidos. Curso Roca 2C 2024

## Instrucciones de uso

Para ejecutar los contenedores usar:

```sh
make docker-run
```

Para detener los contenedores usar:

```sh
make docker-down
```

Para ver los logs actuales del proyecto usar:
```sh
make docker-logs
```

## Comparar resultados

Para poder comparar los resultados,  usar:

```bash
> ./compare_results.sh <client_id> [--re-run]
```

Aclaración: El flag corre todo el notebook de nuevo, sino se usa la data ya existente 