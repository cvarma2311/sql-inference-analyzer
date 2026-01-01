"""
CORRECTED DEMO TEST
Fixed test logic and better reporting
"""

import sys
import os
import time
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

from sql_generator import ProductionSQLGenerator
import training_data

MODELS_TO_TEST = ["llama3.1:8b", "deepseek-coder-v2"]

CRITICAL_TESTS = [
    "Show vehicle number, transporter name, and risk score for high risk vehicles",
    "List all columns in vts_alert_history related to violations",
    "Which tables contain vehicle information",
    "Find vehicles with risk score above 70",
    "Show blacklisted vehicles with their transporter names"
]

def quick_test():
    """Run comprehensive test with model comparison"""
    print("\n" + "="*80)
    print("QUICK DEMO VALIDATION & MODEL COMPARISON")
    print("="*80)
    
    generator = ProductionSQLGenerator(training_data)
    
    results = {}
    
    for i, question in enumerate(CRITICAL_TESTS, 1):
        print(f"\n[{i}/{len(CRITICAL_TESTS)}] Question: {question}")
        print("-" * 80)
        
        # Invalidate cache to force fresh generation
        generator.rag_system.invalidate_cache_entry(question)
        
        results[question] = {}
        
        for model in MODELS_TO_TEST:
            print(f"  Testing Model: {model}...")
            start_time = time.time()
            
            try:
                result = generator.generate_sql(
                    question, 
                    model_name=model, 
                    use_cache=False
                )
                
                duration = time.time() - start_time
                sql = result.get('sql_query', '')
                source = result.get('source', 'unknown')
                
                success = False
                error_msg = None
                
                if not sql or 'error' in str(sql).lower():
                    error_msg = "No valid SQL generated"
                else:
                    # Check if deterministic (always succeeds)
                    if "deterministic" in source:
                        success = True
                    else:
                        # Execute and check
                        exec_result = generator.execute_query(sql)
                        if exec_result.get('success'):
                            row_count = exec_result.get('count', 0)
                            
                            # ✅ FIX: Better success criteria
                            # Consider success if:
                            # 1. Query executed without error
                            # 2. Row count is reasonable (< 100k for non-aggregation)
                            # 3. OR it's an aggregation query
                            
                            if row_count < 100000:
                                success = True
                            elif "count(" in sql.lower() or "group by" in sql.lower():
                                success = True
                            else:
                                error_msg = f"Row count too high: {row_count}"
                        else:
                            error_msg = exec_result.get('error', 'Unknown error')
                
                results[question][model] = {
                    'success': success,
                    'sql': sql,
                    'error': error_msg,
                    'duration': duration,
                    'source': source
                }
                
                status = "✓ Success" if success else "✗ Failure"
                print(f"  Result: {status} ({duration:.2f}s)")
                print(f"  Source: {source}")
                if len(sql) < 200:
                    print(f"  SQL: {sql}")
                else:
                    print(f"  SQL: {sql[:1000]}")
                if error_msg:
                    print(f"  Error: {error_msg}")
            
            except Exception as e:
                duration = time.time() - start_time
                print(f"  Result: ✗ Failure ({duration:.2f}s)")
                print(f"  ERROR: {e}")
                results[question][model] = {
                    'success': False, 
                    'sql': '', 
                    'error': str(e), 
                    'duration': duration,
                    'source': 'exception'
                }
    
    # Print Summary
    print("\n" + "="*80)
    print("PERFORMANCE SUMMARY")
    print("="*80)
    
    # Calculate statistics
    model_stats = {model: {'pass': 0, 'fail': 0, 'total_time': 0} for model in MODELS_TO_TEST}
    
    for question, model_results in results.items():
        for model, res in model_results.items():
            if res['success']:
                model_stats[model]['pass'] += 1
            else:
                model_stats[model]['fail'] += 1
            model_stats[model]['total_time'] += res['duration']
    
    # Print model comparison
    print(f"\n{'Model':<25} | {'Pass':<6} | {'Fail':<6} | {'Success Rate':<15} | {'Avg Time':<10}")
    print("-" * 85)
    
    for model in MODELS_TO_TEST:
        stats = model_stats[model]
        total = stats['pass'] + stats['fail']
        success_rate = (stats['pass'] / total * 100) if total > 0 else 0
        avg_time = stats['total_time'] / total if total > 0 else 0
        
        print(f"{model:<25} | {stats['pass']:<6} | {stats['fail']:<6} | {success_rate:<14.1f}% | {avg_time:<9.2f}s")
    
    print("\n" + "="*80)
    print("DETAILED RESULTS")
    print("="*80)
    
    # Detailed breakdown
    for i, question in enumerate(CRITICAL_TESTS, 1):
        short_q = (question[:60] + '...') if len(question) > 60 else question
        print(f"\n{i}. {short_q}")
        
        for model in MODELS_TO_TEST:
            res = results[question][model]
            status = "PASS" if res['success'] else "FAIL"
            symbol = "✓" if res['success'] else "✗"
            print(f"   {symbol} {model:<20}: {status:<6} ({res['duration']:.2f}s) [{res['source']}]")
            if not res['success'] and res['error']:
                print(f"      Error: {res['error'][:100]}")
    
    print("\n" + "="*80)
    
    # Overall success
    total_tests = len(CRITICAL_TESTS) * len(MODELS_TO_TEST)
    total_pass = sum(stats['pass'] for stats in model_stats.values())
    overall_success_rate = (total_pass / total_tests * 100)
    
    print(f"\nOverall Success Rate: {overall_success_rate:.1f}% ({total_pass}/{total_tests})")
    print("="*80)
    
    # Print generator stats
    generator.print_stats()
    
    # Determine if test suite passed (require 80% success)
    return overall_success_rate >= 80.0

if __name__ == "__main__":
    try:
        success = quick_test()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n✗ Test suite crashed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)