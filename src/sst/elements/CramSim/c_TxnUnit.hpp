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

/*
 * c_TxnConverter.hpp
 *
 *  Created on: May 18, 2016
 *      Author: tkarkha
 */

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#ifndef C_TxnConverter_HPP_
#define C_TxnConverter_HPP_

#include <vector>
#include <queue>

// SST includes
#include <sst/core/subcomponent.h>
#include <sst/core/link.h>

// local includes
#include "c_BankCommand.hpp"
#include "c_TransactionToCommands.hpp"
#include "c_CmdUnit.hpp"
#include "c_CtrlSubComponent.hpp"
#include "c_CmdScheduler.hpp"
#include "c_Transaction.hpp"

namespace SST {
namespace n_Bank {
	class c_CmdScheduler;
class c_TxnConverter: public c_CtrlSubComponent <c_Transaction*,c_BankCommand*> {

public:

	c_TxnConverter(SST::Component * comp, SST::Params& x_params);
	~c_TxnConverter();

	void push(c_Transaction* newTxn); // receive txns from txnGen into req q
	bool clockTic(SST::Cycle_t);
	void print() const; // print internals

private:

	c_CmdScheduler *m_nextSubComponent;

	void run();
	void send();
	void createRefreshCmds();

	// FIXME: Remove. For testing purposes
	void printQueues();

	// params for internal architecture
	int k_txnReqQEntries;
	int k_relCommandWidth; // txn relative command width
	bool k_useReadA;
	bool k_useWriteA;
	bool k_useRefresh;
	int k_bankPolicy;

	c_TransactionToCommands *m_converter;

	int k_REFI;
	int m_currentREFICount;
	std::vector<std::vector<unsigned> > m_refreshGroups;
	uint m_currentRefreshGroup;
	std::queue<c_BankCommand*> m_refreshList;
	bool m_processingRefreshCmds; // Cmd unit is processing refresh commands

  // Statistics
  Statistic<uint64_t>* s_readTxnsRecvd;
  Statistic<uint64_t>* s_writeTxnsRecvd;
  Statistic<uint64_t>* s_totalTxnsRecvd;
  Statistic<uint64_t>* s_reqQueueSize;
  Statistic<uint64_t>* s_resQueueSize;
};
}
}

#endif /* C_TxnConverter_HPP_ */
