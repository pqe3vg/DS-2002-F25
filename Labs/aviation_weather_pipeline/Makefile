.PHONY: all fetch analyze clean

analyze: fetch
	@echo "Starting analysis of METAR data..."
	./analyze_data.sh
	@echo "Analysis complete. CSV report saved to weather_report.csv."

fetch:
	@echo "Starting fetch of METAR data..."
	./fetch_metars.sh
	@echo "Fetch complete. Raw METAR JSON files saved to raw_metars/."

clean:
	@echo "Cleaning up generated files..."
	rm -rf raw_metars/ weather_report.csv
	@echo "Cleanup complete. All generated data has been removed."
