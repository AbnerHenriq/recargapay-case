#!/usr/bin/env python3
"""
Runner principal para executar todos os testes de data quality
das camadas bronze, silver e gold usando Great Expectations
"""

from gold.tests_gold import run_gold_tests
from silver.tests_silver import run_silver_tests
from bronze.tests_bronze import run_bronze_tests

if __name__ == "__main__":
    all_results = {}
    all_results.update(run_bronze_tests())
    all_results.update(run_silver_tests())
    all_results.update(run_gold_tests())

    for table, tests in all_results.items():
        print(f"\nResults for {table}")
        for t in tests:
            if isinstance(t, dict) and "error" in t:
                print(f" ERROR: {t['error']}")
            else:
                status = "PASSED" if t.success else "FAILED"
                print(f" - {t.expectation_type}: {status}")
                
                if not t.success:
                    unexpected_count = getattr(t, 'unexpected_count', 0)
                    if unexpected_count > 0:
                        print(f"     Valores inesperados: {unexpected_count}")
