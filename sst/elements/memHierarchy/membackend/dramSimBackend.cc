
#include <sst_config.h>
#include "membackend/dramSimBackend.h"

using namespace SST;
using namespace SST::MemHierarchy;

DRAMSimMemory::DRAMSimMemory(Component *comp, Params &params) : MemBackend(comp, params){
    std::string deviceIniFilename = params.find_string("device_ini", NO_STRING_DEFINED);
    if(NO_STRING_DEFINED == deviceIniFilename)
        _abort(MemController, "XML must define a 'device_ini' file parameter\n");
    std::string systemIniFilename = params.find_string("system_ini", NO_STRING_DEFINED);
    if(NO_STRING_DEFINED == systemIniFilename)
        _abort(MemController, "XML must define a 'system_ini' file parameter\n");


    unsigned int ramSize = (unsigned int)params.find_integer("mem_size", 0);
    memSystem = DRAMSim::getMemorySystemInstance(
            deviceIniFilename, systemIniFilename, "", "", ramSize);

    DRAMSim::Callback<DRAMSimMemory, void, unsigned int, uint64_t, uint64_t>
        *readDataCB, *writeDataCB;

    readDataCB = new DRAMSim::Callback<DRAMSimMemory, void, unsigned int, uint64_t, uint64_t>(
            this, &DRAMSimMemory::dramSimDone);
    writeDataCB = new DRAMSim::Callback<DRAMSimMemory, void, unsigned int, uint64_t, uint64_t>(
            this, &DRAMSimMemory::dramSimDone);

    memSystem->RegisterCallbacks(readDataCB, writeDataCB, NULL);
}



bool DRAMSimMemory::issueRequest(MemController::DRAMReq *req){
    uint64_t addr = req->baseAddr_ + req->amtInProcess_;
    bool ok = memSystem->willAcceptTransaction(addr);
    if(!ok) return false;
    ok = memSystem->addTransaction(req->isWrite_, addr);
    if(!ok) return false;  // This *SHOULD* always be ok
    ctrl->dbg.debug(_L10_, "Issued transaction for address %" PRIx64 "\n", (Addr)addr);
    dramReqs[addr].push_back(req);
    return true;
}



void DRAMSimMemory::clock(){
    memSystem->update();
}



void DRAMSimMemory::finish(){
    memSystem->printStats(true);
}



void DRAMSimMemory::dramSimDone(unsigned int id, uint64_t addr, uint64_t clockcycle){
    std::deque<MemController::DRAMReq *> &reqs = dramReqs[addr];
    ctrl->dbg.debug(_L10_, "Memory Request for %" PRIx64 " Finished [%zu reqs]\n", (Addr)addr, reqs.size());
    assert(reqs.size());
    MemController::DRAMReq *req = reqs.front();
    reqs.pop_front();
    if(0 == reqs.size())
        dramReqs.erase(addr);

    ctrl->handleMemResponse(req);
}