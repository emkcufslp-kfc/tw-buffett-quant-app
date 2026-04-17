from screening import scan_universe


def run():
    print("Initializing TW Buffett candidate scan...")
    results = scan_universe()

    print(f"Universe size: {results['universe_size']}")
    print(f"Market regime bullish: {results['regime']}")
    print(f"Candidates found: {len(results['selected'])}")

    if results["errors"].empty:
        print("No runtime ticker errors were captured during the scan.")
    else:
        print(f"Ticker errors captured: {len(results['errors'])}")
        print(results["errors"].head(10).to_string(index=False))

    if not results["selected"]:
        print("No stocks met the current quality and valuation criteria.")
        return

    print("\n--- Candidates ---")
    print(results["diagnostics"][results["diagnostics"]["selected"]].to_string(index=False))

    print("\n--- Portfolio Weights ---")
    for stock, weight in results["portfolio"].items():
        print(f"{stock}: {weight:.2%}")

    invested = sum(results["portfolio"].values())
    print(f"Capital deployed: {invested:.2%}")


if __name__ == "__main__":
    run()
