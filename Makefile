build-plt:
	dialyzer --build_plt --output_plt .ddoc_cache.plt \
		--apps erts kernel stdlib

ETS_LRU_PATH ?= ../ets_lru
# Check to see if the provided ETS_LRU_PATH exists
# and set ETS_LRU_SRC if it does
ifneq ($(wildcard $(ETS_LRU_PATH)),)
	ETS_LRU_SRC = --src $(ETS_LRU_PATH)/src
endif

dialyze:
	dialyzer --src src --plt .ddoc_cache.plt --no_native -Werror_handling \
	-Wrace_conditions -Wunmatched_returns -pa ../mem3 -pa ../../apps/couch \
	$(ETS_LRU_SRC)
