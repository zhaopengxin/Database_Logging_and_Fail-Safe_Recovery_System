#include "LogMgr.h"
using namespace std;
#include <sstream>


/*
   * Find the LSN of the most recent log record for this TX.
   * If there is no previous log record for this TX, return 
   * the null LSN.
   */
int LogMgr::getLastLSN(int txnum){
	auto txIndex = tx_table.find(txnum);
	if (txIndex == tx_table.end()){
		return NULL_LSN;
	}
	else{
		return tx_table[txnum].lastLSN;
	}

}
/*
   * Update the TX table to reflect the LSN of the most recent
   * log entry for this transaction.
   */
void LogMgr::setLastLSN(int txnum, int lsn){
	tx_table[txnum].lastLSN = lsn;
}
/*
   * Force log records up to and including the one with the
   * maxLSN to disk. Don't forget to remove them from the
   * logtail once they're written!
   */
void LogMgr::flushLogTail(int maxLSN){
	for (auto iter = logtail.begin(); iter != logtail.end();)
	{
		if (((*iter)->getLSN()) <= maxLSN)
		{
			se->updateLog((*iter)->toString());
			iter = logtail.erase(iter);
		}else{
			iter++;
		}
	}
}
/* 
   * Run the analysis phase of ARIES.
   */
void LogMgr::analyze(vector <LogRecord*> log){
	int masterLog = se->get_master();
	for (auto iter = log.begin(); iter != log.end(); ++iter)
	{
		/* code */
		// if there exists a checkpoint
		if(((*iter)->getType()) == END_CKPT && (((*iter)->getLSN()) > masterLog)){
			ChkptLogRecord* checkPointLog = dynamic_cast<ChkptLogRecord *>(*iter);
			tx_table = checkPointLog->getTxTable();
			dirty_page_table = checkPointLog->getDirtyPageTable();
		}
	}

	for(auto iter = log.begin(); iter != log.end(); ++iter){
		if(((*iter)->getLSN()) > masterLog){
			
			if((*iter)->getType() == UPDATE){
				// need to add to tx_table if not already exists
				// set last lsn to last lsn table
				if(tx_table.find((*iter)->getTxID()) == tx_table.end()){
					tx_table[(*iter)->getTxID()] = txTableEntry((*iter)->getLSN(),U);
				}
				setLastLSN((*iter)->getTxID(),(*iter)->getLSN());
				// update the dirty page table
				UpdateLogRecord* updateLogRecord = dynamic_cast<UpdateLogRecord*> (*iter);
				int updatePageId = updateLogRecord->getPageID();
				if (dirty_page_table.find(updatePageId) == dirty_page_table.end()){
					dirty_page_table[updatePageId] = (*iter)->getLSN();
				}
			}
			else if((*iter)->getType() == COMMIT){
				tx_table[(*iter)->getTxID()].status = C;
				setLastLSN((*iter)->getTxID(), (*iter)->getLSN());
			}else if((*iter)->getType() == CLR){
				if(tx_table.find((*iter)->getTxID()) == tx_table.end()){
					tx_table[(*iter)->getTxID()] = txTableEntry((*iter)->getLSN(),U);
				}
				setLastLSN((*iter)->getTxID(),(*iter)->getLSN());
				CompensationLogRecord* clrLog = dynamic_cast<CompensationLogRecord*> (*iter);
				int clrlogPageId = clrLog->getPageID();
				if (dirty_page_table.find(clrlogPageId) == dirty_page_table.end()){
					dirty_page_table[clrlogPageId] = (*iter)->getLSN();
				}
			}else if((*iter)->getType() == END){
				// need to remove the relativate record from dirty page
				setLastLSN((*iter)->getTxID(),(*iter)->getLSN());
				int transcation_id = (*iter)->getTxID();
				tx_table.erase(transcation_id);
			}
		}
	}
}

  /*
   * Run the redo phase of ARIES.
   * If the StorageEngine stops responding, return false.
   * Else when redo phase is complete, return true. 
   */
bool LogMgr::redo(vector <LogRecord*> log){
	auto firstRecord = dirty_page_table.begin();
	int earliest_log = firstRecord->second;
	
	for(auto iter = dirty_page_table.begin(); iter != dirty_page_table.end(); ++iter){
		if(earliest_log > iter->second){
			earliest_log = iter->second;
		}
	}
	for(auto iter = log.begin(); iter != log.end(); ++iter){
		if((*iter)->getLSN() >= earliest_log){
			if((*iter)->getType() == UPDATE){
				UpdateLogRecord* updateLog = dynamic_cast<UpdateLogRecord*> (*iter);
				int updatePageId = updateLog->getPageID();
				if(dirty_page_table.find(updatePageId) == dirty_page_table.end()){
					continue;
				}
				int recLSN = dirty_page_table.find(updatePageId)->second;
				int updatePageLsn = updateLog->getLSN();
				if(recLSN > updatePageLsn){
					continue;
				}
				int pageLSN = se->getLSN(updatePageId);
				if(pageLSN >= updatePageLsn){
					continue;
				}
				if(!se->pageWrite(updatePageId, updateLog->getOffset(), updateLog->getAfterImage(), updatePageLsn)){
					return false;
				}
			}
			else if((*iter)->getType() == CLR){
				CompensationLogRecord* clrLog = dynamic_cast<CompensationLogRecord*> (*iter);
				int clrPageId = clrLog->getPageID();
				if(dirty_page_table.find(clrPageId) == dirty_page_table.end()){
					continue;
				}
				int recLSN = dirty_page_table.find(clrPageId)->second;
				int clrPageLsn = clrLog->getLSN();
				if(recLSN > clrPageLsn){
					continue;
				}
				int pageLSN = se->getLSN(clrPageId);
				if(pageLSN >= clrPageLsn){
					continue;
				}
				if(!se->pageWrite(clrPageId, clrLog->getOffset(), clrLog->getAfterImage(), clrPageLsn)){
					return false;
				}
			}// end check CLR
		}// end check for the earliest log
	}// end iternation
	for(auto iter = tx_table.begin(); iter != tx_table.end(); ){
		if(iter->second.status == C){
			int lsn = se->nextLSN();
            int prevlsn = iter->second.lastLSN;
			LogRecord* endLog = new LogRecord(lsn, prevlsn, iter->first, END);
            logtail.push_back(endLog);
            iter = tx_table.erase(iter);
		}else{
			iter++;
		}
	}
	return true;
}

  /*
   * If no txnum is specified, run the undo phase of ARIES.
   * If a txnum is provided, abort that transaction.
   * Hint: the logic is very similar for these two tasks!
   */
void LogMgr::undo(vector <LogRecord*> log, int txnum){
	if(txnum == NULL_TX){
		
        map<int, LogRecord*> toUndo;

        for(auto iter = tx_table.begin(); iter != tx_table.end(); ++iter){
        	if(iter->second.status != C){
        		int recentLsn = iter->second.lastLSN;
        		bool isUp = false;
        		for(auto isUpdate = log.rbegin(); isUpdate != log.rend(); ++isUpdate){
        			if((*isUpdate)->getLSN() == recentLsn){
        				if((*isUpdate)->getType() == UPDATE){
        					isUp = true;
        				}
        				break;
        			}
        		}
        		if(isUp){
        			int updatelog = getLastLSN(iter->first);
        			while(updatelog != NULL_LSN){
        				auto ite = log.rbegin();
		        		for(; ite != log.rend(); ++ite){
		        			if((*ite)->getLSN() == updatelog){
		        				break;
		        			}
		        		}
		        		toUndo[(*ite)->getLSN()] = (*ite);
		        		updatelog = (*ite)->getprevLSN();
	        		}
        		}else{
        			auto it = log.rbegin();
        			for(; it != log.rend(); ++it){
        				if((*it)->getLSN() == recentLsn) break;
        			}
        			CompensationLogRecord* clr = dynamic_cast<CompensationLogRecord*> (*it);
        			if(clr->getUndoNextLSN() != NULL_LSN){

	        			for(auto ii = log.begin(); ii != log.end(); ++ii){
	                    	if((*ii)->getTxID() == iter->first && (*ii)->getLSN() <= clr->getUndoNextLSN()){
	                    		toUndo[(*ii)->getLSN()] = (*ii);
	                    	}
	                    }
	                }else if(clr->getUndoNextLSN() == NULL_LSN){
	                	int elsn = se->nextLSN();
	                	int prev_lsn = clr->getLSN();
	                	int txid = clr->getTxID();
	                	LogRecord* endLog = new LogRecord(elsn, prev_lsn, txid, END);
	                    logtail.push_back(endLog);
	                    setLastLSN(txid, prev_lsn);
	                    tx_table.erase(txid);
	                }
        		}
        	}
        }
        

        while (!toUndo.empty()) {
            auto top = toUndo.rbegin();
            
                     
            if ((top->second)->getType() == UPDATE) {
            	UpdateLogRecord* upLog = dynamic_cast<UpdateLogRecord*>(top->second);
                txnum = upLog->getTxID();
                int lsn = se->nextLSN();
                int prev_lsn = getLastLSN(txnum);
                
        		CompensationLogRecord* clrLog = new CompensationLogRecord(lsn, prev_lsn, txnum, upLog->getPageID(), upLog->getOffset(), upLog->getBeforeImage(), upLog->getprevLSN());
                logtail.push_back(clrLog);
                setLastLSN(txnum, lsn);
               	if (!se->pageWrite(upLog->getPageID(), upLog->getOffset(), upLog->getBeforeImage(), lsn)) return;
                if ((top->second)->getprevLSN() == NULL_LSN) {
                    int elsn = se->nextLSN();
                    int prev_lsn = lsn;
                    LogRecord* endLog = new LogRecord(elsn, prev_lsn, txnum, END);
                    logtail.push_back(endLog);
                    setLastLSN(txnum, lsn);
                    tx_table.erase(txnum);
                    //flushLogTail(lsn);
                }

            }
            if (!toUndo.empty()) {
                toUndo.erase(top->first);
            }
        }
	}// end recover scenario
	else if(txnum != NULL_TX){
		vector<LogRecord*> ToUndo;
		
        for (auto iter = log.rbegin(); iter != log.rend(); ++iter) {
            if ((*iter)->getTxID() == txnum) {
            	if ((*iter)->getType() == END)
                {
                	break;
                }
                if ((*iter)->getType() == UPDATE) {
                    ToUndo.push_back(*iter);
                }
            }
        }
		// undo according ToUndo List
		for(auto toDoiter = ToUndo.begin(); toDoiter != ToUndo.end(); ++toDoiter){
			TxType toDoType = (*toDoiter)->getType();
			//int finallsn = NULL_LSN;
			if(toDoType == UPDATE){
				UpdateLogRecord* todoUpdateLogRecord = dynamic_cast<UpdateLogRecord*> (*toDoiter);
				int todoUpdateTxid = todoUpdateLogRecord->getTxID();
				int todoUpdatePageId = todoUpdateLogRecord->getPageID();
				int todoUpdateOffset = todoUpdateLogRecord->getOffset();
				string todoImg = todoUpdateLogRecord->getBeforeImage();
				int undoNext = todoUpdateLogRecord->getprevLSN();
				int lsn = se->nextLSN();
				//finallsn = lsn;
				int prevlsn = getLastLSN(todoUpdateTxid);
				
				CompensationLogRecord* CLRRecord = new CompensationLogRecord(lsn, prevlsn, 
				todoUpdateTxid, todoUpdatePageId, todoUpdateOffset, todoImg, undoNext);
				logtail.push_back(CLRRecord);
				setLastLSN(todoUpdateTxid, lsn);
				if(!se->pageWrite(todoUpdatePageId, todoUpdateOffset, todoImg, lsn)) return;

				if(undoNext == NULL_LSN){
					LogRecord* endLog = new LogRecord(se->nextLSN(), lsn, todoUpdateTxid, END);
					logtail.push_back(endLog);
					setLastLSN(todoUpdateTxid, lsn);
					tx_table.erase(todoUpdateTxid);
					//flushLogTail(lsn);
				}
				
			}
		}// end list
		
	}// end abort scenario
}
/*
   * Abort the specified transaction.
   * Hint: you can use your undo function
   */
void LogMgr::abort(int txid){
	int lsn = se->nextLSN();
    int prevlsn = getLastLSN(txid);
    LogRecord* abortLog = new LogRecord(lsn, prevlsn, txid, ABORT);
    logtail.push_back(abortLog);
    vector<LogRecord*> log_disk = stringToLRVector(se->getLog());
    log_disk.insert(log_disk.end(), logtail.begin(), logtail.end());
    setLastLSN(txid, lsn);
    undo(log_disk, txid);
}

  /*
   * Write the begin checkpoint and end checkpoint
   */
void LogMgr::checkpoint(){
	int beginCheck = se->nextLSN();
	int prevlsn = NULL_LSN;
	int txid = NULL_TX;
	// update the begin checkpoint to logtail
	LogRecord* beginCheckLog = new LogRecord(beginCheck, prevlsn, txid, BEGIN_CKPT);
	logtail.push_back(beginCheckLog);
	// put the lsn of begin checkpoint to master 
	se->store_master(beginCheck);
	// update the end checkpoint to logtail
	int endCheck = se->nextLSN();
	LogRecord* endCheckLog = new ChkptLogRecord(endCheck, beginCheck, txid, tx_table, dirty_page_table);
	logtail.push_back(endCheckLog);
	//flush the log to disk
	flushLogTail(endCheck);
}

  /*
   * Commit the specified transaction.
   */
void LogMgr::commit(int txid){
	int lsn = se->nextLSN();
	int prevlsn = getLastLSN(txid);
	LogRecord* commitLog = new LogRecord(lsn, prevlsn, txid, COMMIT);
	logtail.push_back(commitLog);
	setLastLSN(txid, lsn);
	tx_table[txid].status = C;
	flushLogTail(lsn);

	int endLsn = se->nextLSN();
	LogRecord* endLog = new LogRecord(endLsn, lsn, txid, END);
	logtail.push_back(endLog);
	setLastLSN(txid, lsn);
	tx_table.erase(txid);
}

  /*
   * A function that StorageEngine will call when it's about to 
   * write a page to disk. 
   * Remember, you need to implement write-ahead logging
   */
void LogMgr::pageFlushed(int page_id){
	dirty_page_table.erase(page_id);
	flushLogTail(se->getLSN(page_id));
}
int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext){
	
 	int lsn = se->nextLSN();
 	int prevLSN = getLastLSN(txid);
 	UpdateLogRecord* updateLogRecord = new UpdateLogRecord(lsn, prevLSN, txid, page_id, offset, oldtext, input);
 	// update the log to logtail
 	logtail.push_back(updateLogRecord);
 	// update the tx_table
 	if(tx_table.find(txid) == tx_table.end()){
		tx_table[txid] = txTableEntry(lsn,U);
	}
 	setLastLSN(txid, lsn);
 	// update the dirty_page_table
 	if (dirty_page_table.find(page_id) == dirty_page_table.end()){
 		dirty_page_table[page_id] = lsn;
 	}
 	return lsn;
}

vector<LogRecord*> LogMgr::stringToLRVector(string logstring) {
    vector<LogRecord*> result;
    istringstream stream(logstring);
    string line;
    while (getline(stream, line)) {
        LogRecord *lr = LogRecord::stringToRecordPtr(line);
        result.push_back(lr);
    }
    return result;
}
void LogMgr::recover(string log) {
    vector<LogRecord*> log_recover = stringToLRVector(log);
    analyze(log_recover);
    if (redo(log_recover)){
    	undo(log_recover);
	}
}	
void LogMgr::setStorageEngine(StorageEngine* engine) {
	se = engine;
}