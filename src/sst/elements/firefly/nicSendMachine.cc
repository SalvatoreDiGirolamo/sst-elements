// Copyright 2009-2017 Sandia Corporation. Under the terms
// of Contract DE-NA0003525 with Sandia Corporation, the U.S.
// Government retains certain rights in this software.
// 
// Copyright (c) 2009-2017, Sandia Corporation
// All rights reserved.
// 
// Portions are copyright of other developers:
// See the file CONTRIBUTORS.TXT in the top level directory
// the distribution for more information.
//
// This file is part of the SST software package. For license
// information, see the LICENSE file in the top level directory of the
// distribution.


#include "sst_config.h"
#include "nic.h"

using namespace SST;
using namespace SST::Firefly;
using namespace SST::Interfaces;

void Nic::SendMachine::streamInit( SendEntryBase* entry )
{
    MsgHdr hdr;

#ifdef NIC_SEND_DEBUG
    ++m_msgCount;
#endif
    hdr.op= entry->getOp();

    m_dbg.debug(CALL_INFO,1,NIC_DBG_SEND_MACHINE,
        "%p setup hdr, srcPid=%d, destNode=%d dstPid=%d bytes=%lu\n", entry,
        entry->local_vNic(), entry->dest(), entry->dst_vNic(), entry->totalBytes() ) ;

    FireflyNetworkEvent* ev = new FireflyNetworkEvent(m_pktOverhead );
    ev->setDestPid( entry->dst_vNic() );
    ev->setSrcPid( entry->local_vNic() ); 
    ev->setHdr();
    if ( entry->isCtrl() || entry->isAck() ) {
        ev->setCtrl();
    } 

    ev->bufAppend( &hdr, sizeof(hdr) );
    ev->bufAppend( entry->hdr(), entry->hdrSize() );

    m_nic.schedCallback( std::bind( &Nic::SendMachine::getPayload, this, entry, ev ), entry->txDelay() );
}

void Nic::SendMachine::getPayload( SendEntryBase* entry, FireflyNetworkEvent* ev ) 
{
    int pid = entry->local_vNic(); 
    ev->setDestPid( entry->dst_vNic() );
    ev->setSrcPid( pid );
    if ( ! m_inQ->isFull() ) {
	    std::vector< MemOp >* vec = new std::vector< MemOp >; 
        entry->copyOut( m_dbg, m_packetSizeInBytes, *ev, *vec ); 
        m_dbg.debug(CALL_INFO,2,NIC_DBG_SEND_MACHINE, "enque load from host, %lu bytes\n",ev->bufSize());
        if ( entry->isDone() ) {
            ev->setTail();
            m_inQ->enque( m_unit, pid, vec, ev, entry->dest(), std::bind( &Nic::SendMachine::streamFini, this, entry ) );
        } else {
            m_inQ->enque( m_unit, pid, vec, ev, entry->dest() );
            m_nic.schedCallback( std::bind( &Nic::SendMachine::getPayload, this, entry, new FireflyNetworkEvent(m_pktOverhead) ), 0);
        }

    } else {
        m_dbg.debug(CALL_INFO,2,NIC_DBG_SEND_MACHINE, "blocked by host\n");
        m_inQ->wakeMeUp( std::bind( &Nic::SendMachine::getPayload, this, entry, ev ) );
    }
}
void Nic::SendMachine::streamFini( SendEntryBase* entry ) 
{
    if ( m_I_manage ) {
        m_sendQ.pop_front();
        if ( ! m_sendQ.empty() )  {
            streamInit( m_sendQ.front() );
        }
    } else {
        m_activeEntry = NULL;
        m_nic.notifySendDone( this, entry );
    }

    if ( entry->shouldDelete() ) {
        m_dbg.debug(CALL_INFO,1,NIC_DBG_SEND_MACHINE, "%p delete SendEntry entry, pid=%d\n",entry, entry->local_vNic());
        delete entry;
    } else {
        m_dbg.debug(CALL_INFO,1,NIC_DBG_SEND_MACHINE, "%p pid=%d\n",entry,m_id);
    }

}

void  Nic::SendMachine::InQ::enque( int unit, int pid, std::vector< MemOp >* vec,
            FireflyNetworkEvent* ev, int dest, Callback callback )
{
    ++m_numPending;

    m_dbg.verbosePrefix(prefix(), CALL_INFO,2,NIC_DBG_SEND_MACHINE, "get timing for packet %" PRIu64 " size=%lu numPending=%d\n",
                 m_pktNum,ev->bufSize(), m_numPending);

    m_nic.dmaRead( unit, pid, vec,
		std::bind( &Nic::SendMachine::InQ::ready, this,  ev, dest, callback, m_pktNum++ )
    ); 
	// don't put code after this, the callback may be called serially
}

void Nic::SendMachine::InQ::ready( FireflyNetworkEvent* ev, int dest, Callback callback, uint64_t pktNum )
{
    m_dbg.verbosePrefix(prefix(),CALL_INFO,2,NIC_DBG_SEND_MACHINE, "packet %" PRIu64 " is ready, expected=%" PRIu64 "\n",pktNum,m_expectedPkt);
    assert(pktNum == m_expectedPkt++);

    if ( m_pendingQ.empty() && ! m_outQ->isFull() ) {
        ready2( ev, dest, callback );
    } else  {
        m_dbg.verbosePrefix(prefix(),CALL_INFO,2,NIC_DBG_SEND_MACHINE, "blocked by OutQ\n");
        m_pendingQ.push_back( Entry( ev, dest,callback,pktNum ) );
        if ( 1 == m_pendingQ.size() )  {
            m_outQ->wakeMeUp( std::bind( &Nic::SendMachine::InQ::processPending, this ) );
        }
    }
}

void Nic::SendMachine::InQ::ready2( FireflyNetworkEvent* ev, int dest, Callback callback )
{
    m_dbg.verbosePrefix(prefix(),CALL_INFO,2,NIC_DBG_SEND_MACHINE, "pass packet to OutQ numBytes=%lu\n", ev->bufSize() );
    --m_numPending;
    m_outQ->enque( ev, dest );
    if ( callback ) {
        callback();
    }
    if ( m_callback ) {
        m_dbg.verbosePrefix(prefix(),CALL_INFO,2,NIC_DBG_SEND_MACHINE, "wakeup send machine\n");
        m_callback( );
        m_callback = NULL;
    }
}

void Nic::SendMachine::InQ::processPending( )
{
    m_dbg.verbosePrefix(prefix(),CALL_INFO,2,NIC_DBG_SEND_MACHINE, "size=%lu\n", m_pendingQ.size());
    Entry& entry = m_pendingQ.front();
    ready2( entry.ev, entry.dest, entry.callback );

    m_pendingQ.pop_front();
    if ( ! m_pendingQ.empty() ) {
        if ( ! m_outQ->isFull() ) {
            m_dbg.verbosePrefix(prefix(),CALL_INFO,2,NIC_DBG_SEND_MACHINE, "schedule next\n");
            m_nic.schedCallback( std::bind( &Nic::SendMachine::InQ::processPending, this ) );
        } else {
            m_dbg.verbosePrefix(prefix(),CALL_INFO,2,NIC_DBG_SEND_MACHINE, "schedule wakeup\n");
            m_outQ->wakeMeUp( std::bind( &Nic::SendMachine::InQ::processPending, this ) );
        }
    }
}

void Nic::SendMachine::OutQ::enque( FireflyNetworkEvent* ev, int dest )
{
    m_dbg.verbosePrefix(prefix(),CALL_INFO,2,NIC_DBG_SEND_MACHINE, "size=%lu\n", m_queue.size());
    m_queue.push_back( std::make_pair(ev,dest) );
    m_nic.notifyHavePkt(m_id);
}
