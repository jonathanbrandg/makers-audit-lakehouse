#!/usr/bin/env bash
# Muestra el resultado de la prueba end-to-end en lenguaje simple.
# Uso (desde la raiz del repo):
#   bash scripts/resultado.sh               # ultimo resultado
#   bash scripts/resultado.sh --actualizar  # volver a verificar ahora (sin generar trafico) y correr dbt
set -u

if [ "${1:-}" = "--actualizar" ]; then
  echo "Verificando de nuevo con los datos actuales (1-2 minutos)..."
  E2E_SKIP_TRAFFIC=true docker compose up -d --no-deps --force-recreate e2e >/dev/null 2>&1
  docker compose wait e2e >/dev/null
  docker compose up -d --no-deps --force-recreate dbt >/dev/null 2>&1
  docker compose wait dbt >/dev/null
fi

status() { # imprime "estado codigo" del contenedor de un servicio, o "missing"
  local id
  id=$(docker compose ps -a -q "$1")
  [ -z "$id" ] && { echo "missing"; return; }
  docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' "$id"
}

verdict() { # $1 = servicio, $2 = descripcion
  read -r state code <<<"$(status "$1")"
  case "$state" in
    missing) echo "  [--] $2: no se ejecuto (correr: docker compose up -d --build)"; return 1 ;;
    exited)  if [ "$code" = "0" ]; then echo "  [OK] $2"; return 0; fi
             echo "  [X]  $2: FALLO (codigo $code). Detalle: docker compose logs $1"; return 1 ;;
    *)       echo "  [..] $2: todavia en ejecucion, espera y volve a correr este script"; return 1 ;;
  esac
}

echo
echo "RESULTADO DE LA PRUEBA DE CONCEPTO"
echo

docker compose logs --no-log-prefix e2e 2>/dev/null | sed -n 's/^>> /  /p'

done_line=$(docker compose logs --no-log-prefix dbt 2>/dev/null | grep -E 'Done\. PASS=' | tail -1)
if [ -n "$done_line" ]; then
  pass=$(sed -E 's/.*PASS=([0-9]+).*/\1/' <<<"$done_line")
  err=$(sed -E 's/.*ERROR=([0-9]+).*/\1/' <<<"$done_line")
  total=$(sed -E 's/.*TOTAL=([0-9]+).*/\1/' <<<"$done_line")
  echo
  echo "  Tests de calidad de datos (dbt): $pass de $total aprobados, $err con error"
fi

echo
ok=0
verdict e2e "E2E  - SQL Server llega completo al lakehouse, sin perdidas ni duplicados" || ok=1
verdict dbt "dbt  - capas silver y gold construidas y validadas" || ok=1
echo
if [ "$ok" = "0" ]; then echo "VEREDICTO: APROBADA"; else echo "VEREDICTO: NO APROBADA (ver detalle arriba)"; fi
exit "$ok"
