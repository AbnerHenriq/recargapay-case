#!/usr/bin/env python3
"""
Runner principal para executar todos os testes de data quality
das camadas silver e gold usando Great Expectations
"""

import sys

from gold.tests_gold import run_gold_tests
from silver.tests_silver import run_silver_tests

if __name__ == "__main__":
    all_results = {}
    all_results.update(run_silver_tests())
    all_results.update(run_gold_tests())

    # Contadores para verificar falhas
    total_tests = 0
    failed_tests = 0
    error_tables = 0

    for table, tests in all_results.items():
        print(f"\nResults for {table}")
        for t in tests:
            if isinstance(t, dict) and "error" in t:
                print(f" ❌ ERROR: {t['error']}")
                error_tables += 1
            else:
                total_tests += 1
                status = "✅ PASSED" if t.success else "❌ FAILED"
                print(f" - {t.expectation_type}: {status}")
                
                if not t.success:
                    failed_tests += 1
                    unexpected_count = getattr(t, 'unexpected_count', 0)
                    if unexpected_count > 0:
                        print(f"     ⚠️  Valores inesperados: {unexpected_count}")

    # Resumo final
    print(f"\n{'='*50}")
    print(f"📊 RESUMO EXECUTIVO")
    print(f"{'='*50}")
    print(f"🧪 Total de testes: {total_tests}")
    print(f"✅ Testes aprovados: {total_tests - failed_tests}")
    print(f"❌ Testes reprovados: {failed_tests}")
    print(f"🚨 Tabelas com erro: {error_tables}")
    
    if total_tests > 0:
        success_rate = ((total_tests - failed_tests) / total_tests) * 100
        print(f"📈 Taxa de sucesso: {success_rate:.1f}%")

    # FALHAR O JOB SE HOUVER PROBLEMAS
    if failed_tests > 0 or error_tables > 0:
        print(f"\n🚨 ATENÇÃO: Foram encontradas falhas nos testes!")
        print(f"   - Testes falharam: {failed_tests}")
        print(f"   - Tabelas com erro: {error_tables}")
        print(f"   - Job falhou por questões de data quality")
        sys.exit(1)  # FALHA O JOB
    else:
        print(f"\n🎉 Todos os testes passaram com sucesso!")
        print(f"   - Data quality validada")
        print(f"   - Job executado com sucesso")
        sys.exit(0)  # SUCESSO
