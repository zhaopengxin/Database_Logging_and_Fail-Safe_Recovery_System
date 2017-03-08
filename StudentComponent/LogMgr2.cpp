//EECS 484 project 4
//alyssa keimach (akeimach) and max kiss (maxkiss)

#include "LogMgr.h"
#include <sstream>

//Find the LSN of the most recent log record for this TX
int LogMgr::getLastLSN(int txnum) {
    // if (tx_table.count(txnum)) return tx_table[txnum].lastLSN;
    // return NULL_LSN;
    auto txIndex = tx_table.find(txnum);
    if (txIndex == tx_table.end()){
        return NULL_LSN;
    }
    else{
        return tx_table[txnum].lastLSN;
    }
}

//Update TX table to reflect LSN of most recent log entry for this transaction
void LogMgr::setLastLSN(int txnum, int lsn) {
    tx_table[txnum].lastLSN = lsn;
}

//Sets this.se to engine
void LogMgr::setStorageEngine(StorageEngine* engine) {
    se = engine;
}

//If no txnum is specified, run the undo phase of ARIES
vector<LogRecord*> LogMgr::stringToLRVector(string logstring) {
    vector<LogRecord*> result;
    istringstream stream(logstring);
    string line;
    while (getline(stream, line)) {
        LogRecord* lr = LogRecord::stringToRecordPtr(line);
        result.push_back(lr);
    }
    return result;
}

//Logs update to database, updates tables, return pageLSN
int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext) {
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
    // int lsn = se->nextLSN();
    // int prev_lsn = getLastLSN(txid);
    // UpdateLogRecord* updateLog = new UpdateLogRecord(lsn, prev_lsn, txid, page_id, offset, oldtext, input);
    // logtail.push_back(updateLog);
    // setLastLSN(txid, lsn);
    
    // txTableEntry tx_update = txTableEntry(lsn, U);
    // tx_table[txid] = tx_update;
    
    // if (!dirty_page_table.count(page_id)) {
    //     dirty_page_table[page_id] = lsn;
    // }
    
    // return lsn;
}

//flushLogTail only in pageFlushed(page_id), commit(txid), checkpoint()

//Force log records up to and including the one with the maxLSN to disk
void LogMgr::flushLogTail(int maxLSN) {
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
    // for(vector<LogRecord*>::iterator it = logtail.begin(); it != logtail.end();) {
    //     if ((*it)->getLSN() <= maxLSN) {
    //         se->updateLog((*it)->toString());
    //         logtail.erase(it);
    //     }
    //     else it++;
    // }
}

//StorageEngine will call when it's about to write a page to disk
void LogMgr::pageFlushed(int page_id) {
    int pageLSN = se->getLSN(page_id);
    flushLogTail(pageLSN);
    dirty_page_table.erase(pageLSN);
}

//Commit the specified transaction
void LogMgr::commit(int txid) {
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
    //flushLogTail(lsn);
    tx_table.erase(txid);
    // int lsn = se->nextLSN();
    // int prev_lsn = getLastLSN(txid);
    // LogRecord* commitLog = new LogRecord(lsn, prev_lsn, txid, COMMIT);
    // logtail.push_back(commitLog);
    // setLastLSN(txid, lsn);
    
    // tx_table[txid].status = C;
    // flushLogTail(lsn);
    
    // int lsn_end = se->nextLSN();
    // prev_lsn = lsn;
    // LogRecord* endLog = new LogRecord(lsn_end, prev_lsn, txid, END);
    // logtail.push_back(endLog);
    // setLastLSN(txid, lsn_end);
    
    // tx_table.erase(txid);
}

//Write the begin checkpoint and end checkpoint
void LogMgr::checkpoint() {
    
    int lsn = se->nextLSN();
    int prev_lsn = NULL_LSN;
    LogRecord* beginLog = new LogRecord(lsn, prev_lsn, NULL_TX, BEGIN_CKPT);
    logtail.push_back(beginLog);
    
    se->store_master(lsn);
    
    int lsn_end = se->nextLSN();
    prev_lsn = lsn;
    LogRecord* endCLog = new ChkptLogRecord(lsn_end, prev_lsn, NULL_TX, tx_table, dirty_page_table);
    logtail.push_back(endCLog);
    setLastLSN(NULL_TX, lsn_end);
    
    flushLogTail(lsn_end);
}

//Abort the specified transaction, use your undo function
void LogMgr::abort(int txid) {
    
    // int lsn = se->nextLSN();
    // int prev_lsn = getLastLSN(txid);
    // LogRecord* abortLog = new LogRecord(lsn, prev_lsn, txid, ABORT);
    // logtail.push_back(abortLog);
    // setLastLSN(txid, lsn);
    
    // vector<LogRecord*> log_abort = stringToLRVector(se->getLog());
    // undo(log_abort, txid);
    int lsn = se->nextLSN();
    int prevlsn = getLastLSN(txid);
    LogRecord* abortLog = new LogRecord(lsn, prevlsn, txid, ABORT);
    vector<LogRecord*> log_abort = stringToLRVector(se->getLog());
    logtail.push_back(abortLog);
    log_abort.insert(log_abort.end(), logtail.begin(), logtail.end());
    setLastLSN(txid, lsn);
    undo(log_abort, txid);
}

//Recover from a crash, given the log from the disk
void LogMgr::recover(string log) {
    vector<LogRecord*> log_recover = stringToLRVector(log);
    analyze(log_recover);
    if (redo(log_recover)) undo(log_recover);
}

//Run the analysis phase of ARIES.
//mine
void LogMgr::analyze(vector <LogRecord*> log) {
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
            if((*iter)->getType() == END){
                // need to remove the relativate record from dirty page
                int transcation_id = (*iter)->getTxID();
                tx_table.erase(transcation_id);
            }
            else if((*iter)->getType() == UPDATE){
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
            }
            // still need some for clr
        }
    }
    // //get begin_checkpoint
    // int ckptLSN = se->get_master();
    // for(vector<LogRecord*>::reverse_iterator it = log.rbegin(); it != log.rend(); it++) {
    //     //find most recent begin_ckpt
    //     if (((*it)->getType() == END_CKPT) && ((*it)->getLSN() <= ckptLSN)) {
    //         //initialize dirty page table and tx table to copies of those at end ckpt
    //         ChkptLogRecord* grabChkpt = dynamic_cast<ChkptLogRecord*>(*it);
    //         tx_table = (grabChkpt)->getTxTable();
    //         dirty_page_table = (grabChkpt)->getDirtyPageTable();
    //         break;
    //     }
    // }
    // //scan forward to end of log
    // for(vector<LogRecord*>::iterator it = log.begin(); it != log.end(); it++) {
    //     //start from most recent end checkpoint
    //     if ((*it)->getLSN() > ckptLSN) {
    //         if ((*it)->getType() == END) {
    //             tx_table.erase((*it)->getTxID());
    //         }
    //         else if ((*it)->getType() == UPDATE) {
    //             //needs to be undone
    //             if (UpdateLogRecord* grabUpdate = dynamic_cast<UpdateLogRecord*>(*it)) {
    //                 tx_table[grabUpdate->getTxID()] = txTableEntry(grabUpdate->getLSN(), U);
    //                 if (!dirty_page_table.count(grabUpdate->getPageID())) {
    //                     dirty_page_table[grabUpdate->getPageID()] = grabUpdate->getLSN();
    //                 }
    //                 setLastLSN(grabUpdate->getTxID(), grabUpdate->getLSN());
    //             }
    //         }
    //         else if ((*it)->getType() == COMMIT) {
    //             if (UpdateLogRecord* grabCommit = dynamic_cast<UpdateLogRecord*>(*it)) {
    //                 tx_table[grabCommit->getTxID()] = txTableEntry(grabCommit->getLSN(), C);
    //                 setLastLSN(grabCommit->getTxID(), grabCommit->getLSN());
    //             }
    //         }
    //     }
    // }
}

//Redo phase of ARIES false if SE stops responding, true when redo phase complete
//mine
bool LogMgr::redo(vector <LogRecord*> log) {
    //reapply updates of ALL transactions, committed or otherwise
    //get smallest reclsn of all pages in dpt, new map because they are fucking organized by page_id
        int earliest_log = NULL_LSN;
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
    // int txnum = NULL_TX;
    // int lastlsn = NULL_LSN;
    // //int prev_lsn = NULL_LSN;
    // map<int, int> dirty_lsn;
    // for (map<int, int>::iterator dp = dirty_page_table.begin(); dp != dirty_page_table.end(); dp++) {
    //     dirty_lsn[dp->second] = dp->first;
    // }
    // //new map has oldest update that may not have been written to disk
    // int oldest = dirty_lsn.begin()->first;
    // //scan forward to end of log, for each redoable record, check to see if action should be undone
    // for (vector<LogRecord*>::iterator lr = log.begin(); lr != log.end(); ++lr) {
    //     //redoable records are updates or clrs
    //     if ((*lr)->getLSN() >= lastlsn) {
    //         lastlsn = (*lr)->getLSN();
    //         txnum = (*lr)->getTxID();
    //         setLastLSN(txnum, lastlsn);
    //     }
    //     if ((*lr)->getLSN() >= oldest) {
    //         if (((*lr)->getType() == CLR) || ((*lr)->getType() == UPDATE)) {
    //             //needs to be redone if affected page in dpt
    //             int lsn = NULL_LSN;
    //             int pageid = NULL_TX;
    //             int valid = 0;
    //             UpdateLogRecord* update = dynamic_cast<UpdateLogRecord*>(*lr);
    //             CompensationLogRecord* clr = dynamic_cast<CompensationLogRecord*>(*lr);
    //             if (update) {
    //                 lsn = update->getLSN();
    //                 pageid = update->getPageID();
    //                 valid = 1;
    //             }
    //             if (clr) {
    //                 lsn = clr->getLSN();
    //                 pageid = clr->getPageID();
    //                 valid = 2;
    //             }
    //             if ((valid == 1) || (valid == 2)) {
    //                 //check if the page is in dpt
    //                 for (map<int, int>::iterator dp = dirty_page_table.begin(); dp != dirty_page_table.end(); dp++) {
    //                     if (pageid == dp->first) {
    //                         if (lsn >= dp->second) {
    //                             if (lsn > se->getLSN(dp->first)) {
    //                                 //then reapply the logged action
    //                                 //write function sets last lsn
    //                                 if (valid == 1) { //was an update
                                        
    //                                     int pageLSN = se->getLSN(update->getPageID());
    //                                     if (!se->pageWrite(update->getPageID(), update->getOffset(), update->getAfterImage(), pageLSN)) return false;
    //                                     else {
    //                                         se->pageWrite(update->getPageID(), update->getOffset(), update->getAfterImage(), pageLSN);
    //                                     }
                                        
    //                                 }
    //                                 else if (valid == 2) { //was a clr
    //                                     int pageLSN = se->getLSN(clr->getPageID());
    //                                     if (!se->pageWrite(clr->getPageID(), clr->getOffset(), clr->getAfterImage(), pageLSN)) return false;
    //                                     else {
    //                                         se->pageWrite(clr->getPageID(), clr->getOffset(), clr->getAfterImage(), pageLSN);
    //                                     }
    //                                 }
    //                             }
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //     }
    // }
    // int lsn = se->nextLSN();
    // int prev_lsn = getLastLSN(txnum);
    // LogRecord* endLog = new LogRecord(lsn, prev_lsn, txnum, END);
    // logtail.push_back(endLog);
    // setLastLSN(txnum, lsn);
    // tx_table.erase(txnum);
    // return true;
}

//If no txnum is specified, run the undo phase of ARIES, else abort that transaction
void LogMgr::undo(vector <LogRecord*> log, int txnum) {
    //abort active transactions at time of crash
    //scan backward
    if (txnum == NULL_TX) {
        map<int, LogRecord*> toUndo;
        for (map<int, txTableEntry>::iterator d = tx_table.begin(); d != tx_table.end();) {
            for (vector<LogRecord*>::reverse_iterator it = log.rbegin(); it != log.rend(); ++it) {
                if (tx_table.count((*it)->getTxID())) {
                    if ((*it)->getType() == COMMIT) {
                        tx_table.erase((*it)->getTxID());
                    }
                    else if ((*it)->getType() == END) {
                        tx_table.erase((*it)->getTxID());
                    }
                    else {
                        if (dynamic_cast<UpdateLogRecord*>(*it) || dynamic_cast<CompensationLogRecord*>(*it)) {
                            toUndo[(*it)->getprevLSN()] = (*it);
                        }
                    }
                }
            }
            while (!toUndo.empty()) {
                map<int, LogRecord*>::reverse_iterator top = toUndo.rbegin();
                CompensationLogRecord* clr = dynamic_cast<CompensationLogRecord*>(top->second);
                if (clr && clr->getUndoNextLSN() != NULL_LSN) {
                    for (vector<LogRecord*>::iterator it = log.begin(); it != log.end(); it++) {
                        if (clr->getUndoNextLSN() == (*it)->getLSN()) {
                            toUndo[clr->getUndoNextLSN()] = (*it);
                        }
                    }
                }
                else if (clr && clr->getUndoNextLSN() == NULL_LSN) {
                    int lsn = se->nextLSN();
                    int prev_lsn = NULL_LSN;
                    LogRecord* endLog = new LogRecord(lsn, prev_lsn, txnum, END);
                    logtail.push_back(endLog);
                    setLastLSN(txnum, lsn);
                    tx_table.erase(txnum);
                }
                UpdateLogRecord* up = dynamic_cast<UpdateLogRecord*>(top->second);
                if (up) {
                    txnum = up->getTxID();
                    int lsn = se->nextLSN();
                    int prev_lsn = getLastLSN(txnum);
                    if (!se->pageWrite(up->getPageID(), up->getOffset(), up->getBeforeImage(), prev_lsn)) return;
                    
                    CompensationLogRecord* clrLog = new CompensationLogRecord(lsn, prev_lsn, txnum, up->getPageID(), up->getOffset(), up->getBeforeImage(), up->getprevLSN());
                    logtail.push_back(clrLog);
                    setLastLSN(txnum, lsn);
                }
                if (!toUndo.empty()) {
                    toUndo.erase(top->first);
                }
            }
            if (tx_table.size() > 1) d++;
            else break;
        }
    }
    else if (txnum != NULL_TX) {
        vector<LogRecord*> toUndo;
        for (vector<LogRecord*>::iterator it = log.begin(); it != log.end(); ++it) {
            if (((*it)->getTxID() == txnum) && ((*it)->getType() != END)) {
                if (dynamic_cast<UpdateLogRecord*>(*it) || dynamic_cast<CompensationLogRecord*>(*it)) {
                    toUndo.push_back(*it);
                }
            }
        }
        while (!toUndo.empty()) {
            UpdateLogRecord* update = dynamic_cast<UpdateLogRecord*>(toUndo.back());
            if (update->getprevLSN() == NULL_LSN) {
                
                
                txnum = update->getTxID();
                int lsn = se->nextLSN();
                int prev_lsn = getLastLSN(txnum);
                if (!se->pageWrite(update->getPageID(), update->getOffset(), update->getBeforeImage(), prev_lsn)) return;
                
                CompensationLogRecord* clrLog = new CompensationLogRecord(lsn, prev_lsn, txnum, update->getPageID(), update->getOffset(), update->getBeforeImage(), update->getprevLSN());
                logtail.push_back(clrLog);
                setLastLSN(txnum, lsn);
            }
            else {
                txnum = update->getTxID();
                int lsn = se->nextLSN();
                int prev_lsn = getLastLSN(txnum);
                if (!se->pageWrite(update->getPageID(), update->getOffset(), update->getBeforeImage(), prev_lsn)) return;
                
                CompensationLogRecord* clrLog = new CompensationLogRecord(lsn, prev_lsn, txnum, update->getPageID(), update->getOffset(), update->getBeforeImage(), update->getprevLSN());
                logtail.push_back(clrLog);
                setLastLSN(txnum, lsn);
            }
            if (!toUndo.empty()) {
                toUndo.pop_back();
            }
        }
    }
    int lsn = se->nextLSN();
    int prev_lsn = getLastLSN(txnum);
    LogRecord* endLog = new LogRecord(lsn, prev_lsn, txnum, END);
    logtail.push_back(endLog);
    setLastLSN(txnum, lsn);
    tx_table.erase(txnum);
}

