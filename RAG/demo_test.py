"""
QUICK DEMO TEST - Run before presentation
Tests critical edge cases and compares model performance
"""
import sys
import os
import time
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))

from sql_generator import ProductionSQLGenerator
import training_data

MODELS_TO_TEST = ["mannix/defog-llama3-sqlcoder-8b:latest","llama3.1:8b", "deepseek-coder-v2"]

# --- CONFIGURATION ---
# USE_FALLBACK (Internal Generator Logic):
#   True  = If the requested model fails, the generator will internally try other available models.
#   False = The generator will only try the requested model and fail if it errors.
USE_FALLBACK = True

# TEST_ALL_MODELS (Script Loop Logic):
#   True  = Run the test for EVERY model in the list (Comparison Mode).
#   False = Stop after the first successful generation for a question (Production Simulation).
TEST_ALL_MODELS = True

CRITICAL_TESTS = [
    # "Which tables contain vehicle numbers, and what are the exact column names used",
    # "who is the transporter for vehicle JH06K0762",
    #"give data of JH06K0762 of last 3 months",
    # "List all columns in vts_alert_history related to violations",
    #"Which tables store transporter information",
    # "Show vehicle number, transporter name, and risk score for high risk vehicles",
    #"Which tables contain vehicle information",
    # "Find vehicles with risk score above 70",
    # "Show blacklisted vehicles with their transporter names",
    #"JH06K0762",
    #"Which vehicles had violations in the last 6 months but not in the last 3 months",

    #"List all alerts generated in the last 24 hours"
    #"Which vehicles have not reported any alert in the last 7 days"
    #"Which vehicles have more than 10 total violations in the last 60 days"
    # "Show zone-wise total violations",
    #"Which vehicles are not blacklisted but have no alert data in the last 7 days"
    #"show vehicle risk score that in the last 3 months?"
    #"List all drivers associated with transporter 'MS AMARJEET TRANSPORT COMPANY' who have vehicles with more than 5 alerts in the last month",
    #"What is the average risk score for vehicles in the North zone over the last month?"
    #"what are the common columns in tt_risk_score table and completed_trips_risk_score table"
    #"List all vehicles that had speed violations but are not categorized as high risk"
    "List all transporters who have vehicles with no alerts in the last 30 days"
]

def quick_test():
    """Run quick validation and model comparison"""
    print("\nQUICK DEMO VALIDATION & MODEL COMPARISON")
    print("="*80)
    
    # --- NEW: Force re-indexing of the RAG system's vector store ---
    generator = ProductionSQLGenerator(training_data, force_reindex_rag=True)
    
    results = {}
    
    for i, question in enumerate(CRITICAL_TESTS, 1):
        print(f"\n[{i}/{len(CRITICAL_TESTS)}] Question: {question}")
        print("-" * 80)

        # Invalidate cache for this specific test question to ensure fresh generation from each model
        generator.rag_system.invalidate_cache_entry(question)

        # --- NEW: Tournament Mode Logic ---
        best_sql_for_learning = None

        results[question] = {}
        
        for model in MODELS_TO_TEST:
            print(f"  Testing Model: {model}...")
            start_time = time.time()

            try:
                # --- FIX: Pass the model and fallback flag to the generator ---
                result = generator.generate_sql(
                    question,
                    model_name=model,
                    use_cache=False,
                    allow_fallback=USE_FALLBACK
                )

                duration = time.time() - start_time
                sql = result.get('sql_query', '')
                
                success = False
                error_msg = None
                
                if not sql or 'error' in str(sql).lower():
                    error_msg = "No valid SQL generated"
                else:
                    # The complex validation logic has been moved into the generator's self-correction loop.
                    # The test script's job is now simpler: just execute and check for success.
                    exec_result = generator.execute_query(sql)
                    if exec_result.get('success'):
                        success = True
                    else:
                        error_msg = f"Final SQL failed execution: {exec_result.get('error')}"

                results[question][model] = {
                    'success': success,
                    'sql': sql,
                    'error': error_msg,
                    'duration': duration
                }
                
                print(f"  Result: {'Success' if success else 'Failure'} (Duration: {duration:.2f} seconds)")
                print(f"  SQL Query: {sql}")
                
                if success:
                    source = result.get('source', 'Unknown')
                    print(f"  Generation Source: {source}")
                    data = result.get('data', [])
                    if data:
                        print(f"  Data ({len(data)} rows):")
                        for row in data[:3]:
                            print(f"    {row}")
                        if len(data) > 3:
                            print(f"    ... {len(data) - 3} more rows ...")
                    else:
                        print("  Data: []")
                    
                    # --- NEW: Capture the first successful query for learning ---
                    if best_sql_for_learning is None:
                        best_sql_for_learning = sql
                        print(f"  [Auto-Learning] '{model}' produced the first valid query. It will be learned.")

                if not success:
                    print(f"  Error: {error_msg}")
            
            except Exception as e:
                duration = time.time() - start_time
                print(f"  Result: Failure (Duration: {duration:.2f} seconds)")
                print(f"  ERROR: An exception occurred during generation: {e}")
                results[question][model] = {'success': False, 'sql': '', 'error': str(e), 'duration': duration}
            
            # --- NEW: Break loop if not comparing all models ---
            if results[question][model]['success'] and not TEST_ALL_MODELS:
                print(f"  [Production Mode] Success with {model}. Skipping remaining models.")
                break
        
        # --- NEW: After all models have been tried, learn the single best query ---
        if best_sql_for_learning:
            print(f"\n  [Auto-Learning] Adding the best generated query to RAG...")
            query_type = next((item.get('query_type', 'general') for item in training_data.TRAINING_QA_PAIRS if item['question'] == question), 'general')
            generator.rag_system.add_learned_example(question, best_sql_for_learning, query_type)
        else:
            print(f"\n  [Auto-Learning] No model produced a logically correct query for this question. Nothing will be learned.")

    print("\n" + "="*80)
    print("PERFORMANCE SUMMARY")
    print("="*80)
    print(f"{'Question':<50} | {'Model':<20} | {'Status':<8} | {'Time (s)':<10}")
    print("-" * 95)
    
    overall_success = True
    for q in CRITICAL_TESTS:
        short_q = (q[:47] + '...') if len(q) > 47 else q
        for model in MODELS_TO_TEST:
            res = results.get(q, {}).get(model, {})
            status = "PASS" if res.get('success') else "FAIL"
            time_taken_str = f"{res.get('duration', 0):.2f}"
            print(f"{short_q:<50} | {model:<20} | {status:<8} | {time_taken_str:<10}")
            if not res.get('success'):
                overall_success = False
        print("-" * 95)

    return overall_success
                
if __name__ == "__main__":
    success = quick_test()
    sys.exit(0 if success else 1)