package org.ethereum.vm;

import org.ethereum.crypto.HashUtil;
import org.ethereum.db.ContractDetails;
import org.ethereum.util.ByteUtil;
import org.ethereum.vm.MessageCall.MsgType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.spongycastle.util.encoders.Hex;

import java.math.BigInteger;

import java.nio.ByteBuffer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Stack;

import static org.ethereum.config.SystemProperties.CONFIG;
import static org.ethereum.vm.OpCode.*;

/**
 * The Ethereum Virtual Machine (EVM) is responsible for initialization
 * and executing a transaction on a contract.
 *
 * It is a quasi-Turing-complete machine; the quasi qualification
 * comes from the fact that the computation is intrinsically bounded
 * through a parameter, gas, which limits the total amount of computation done.
 *
 * The EVM is a simple stack-based architecture. The word size of the machine
 * (and thus size of stack item) is 256-bit. This was chosen to facilitate
 * the SHA3-256 hash scheme and  elliptic-curve computations. The memory model
 * is a simple word-addressed byte array. The stack has an unlimited size.
 * The machine also has an independent storage model; this is similar in concept
 * to the memory but rather than a byte array, it is a word-addressable word array.
 *
 * Unlike memory, which is volatile, storage is non volatile and is
 * maintained as part of the system state. All locations in both storage
 * and memory are well-defined initially as zero.
 *
 * The machine does not follow the standard von Neumann architecture.
 * Rather than storing program code in generally-accessible memory or storage,
 * it is stored separately in a virtual ROM interactable only though
 * a specialised instruction.
 *
 * The machine can have exceptional execution for several reasons,
 * including stack underflows and invalid instructions. These unambiguously
 * and validly result in immediate halting of the machine with all state changes
 * left intact. The one piece of exceptional execution that does not leave
 * state changes intact is the out-of-gas (OOG) exception.
 *
 * Here, the machine halts immediately and reports the issue to
 * the execution agent (either the transaction processor or, recursively,
 * the spawning execution environment) and which will deal with it separately.
 *
 * @author Roman Mandeleil
 * @since 01.06.2014
 */
public class VM {

    private static final Logger logger = LoggerFactory.getLogger("VM");
    private static final Logger dumpLogger = LoggerFactory.getLogger("dump");
    private static BigInteger _32_ = BigInteger.valueOf(32);
    private static String logString = "[{}]\t Op: [{}]  Gas: [{}] Deep: [{}]  Hint: [{}]";

    private static BigInteger MAX_GAS = BigInteger.valueOf(Long.MAX_VALUE);


    /* Keeps track of the number of steps performed in this VM */
    private int vmCounter = 0;

    public void step(Program program) {

        if (CONFIG.vmTrace())
            program.saveOpTrace();

        try {
            OpCode op = OpCode.code(program.getCurrentOp());
            if (op == null)
                throw program.new IllegalOperationException();

            program.setLastOp(op.val());
            program.stackRequire(op.require());

            long oldMemSize = program.getMemSize();
            BigInteger newMemSize = BigInteger.ZERO;
            long copySize = 0;
            Stack<DataWord> stack = program.getStack();

            String hint = "";
            long callGas = 0, memWords = 0; // parameters for logging
            long gasCost = GasCost.STEP;
            long gasBefore = program.getGas().longValue();
            int stepBefore = program.getPC();

            // Calculate fees and spend gas
            switch (op) {
                case STOP:
                case SUICIDE:
                    // The ops that don't charge by step
                    gasCost = GasCost.STOP;
                    break;
                case SSTORE:
                    DataWord newValue = stack.get(stack.size() - 2);
                    DataWord oldValue = program.storageLoad(stack.peek());
                    if (oldValue == null && !newValue.isZero())
                        gasCost = GasCost.SSTORE;
                    else if (oldValue != null && newValue.isZero()) {
                        // todo: GASREFUND counter policy

                        // refund step cost policy.
                        program.futureRefundGas(GasCost.REFUND_SSTORE);
                        gasCost = 0;
                    } else
                        gasCost = GasCost.RESET_SSTORE;
                    break;
                case SLOAD:
                    gasCost = GasCost.SLOAD;
                    break;
                case BALANCE:
                    gasCost = GasCost.BALANCE;
                    break;

                // These all operate on memory and therefore potentially expand it:
                case MSTORE:
                    newMemSize = memNeeded(stack.peek(), new DataWord(32));
                    break;
                case MSTORE8:
                    newMemSize = memNeeded(stack.peek(), new DataWord(1));
                    break;
                case MLOAD:
                    newMemSize = memNeeded(stack.peek(), new DataWord(32));
                    break;
                case RETURN:
                    newMemSize = memNeeded(stack.peek(), stack.get(stack.size() - 2));
                    break;
                case SHA3:
                    gasCost = GasCost.SHA3;
                    newMemSize = memNeeded(stack.peek(), stack.get(stack.size() - 2));
                    DataWord size = stack.get(stack.size() - 2);
                    long chunkUsed = (size.longValue() + 31) / 32;
                    gasCost += chunkUsed * GasCost.SHA3_WORD;
                    break;
                case CALLDATACOPY:
                    copySize = stack.get(stack.size() - 3).longValue();
                    newMemSize = memNeeded(stack.peek(), stack.get(stack.size() - 3));
                    break;
                case CODECOPY:
                    copySize = stack.get(stack.size() - 3).longValue();
                    newMemSize = memNeeded(stack.peek(), stack.get(stack.size() - 3));
                    break;
                case EXTCODECOPY:
                    copySize = stack.get(stack.size() - 4).longValue();
                    newMemSize = memNeeded(stack.get(stack.size() - 2), stack.get(stack.size() - 4));
                    break;
                case CALL:
                case CALLCODE:
                    gasCost = GasCost.CALL;
                    DataWord callGasWord = stack.get(stack.size() - 1);
                    if (callGasWord.compareTo(program.getGas()) == 1) {
                        throw program.new OutOfGasException();
                    }
                    callGas = callGasWord.longValue();
                    BigInteger in = memNeeded(stack.get(stack.size() - 4), stack.get(stack.size() - 5)); // in offset+size
                    BigInteger out = memNeeded(stack.get(stack.size() - 6), stack.get(stack.size() - 7)); // out offset+size
                    newMemSize = in.max(out);
                    break;
                case CREATE:
                    gasCost = GasCost.CREATE;
                    newMemSize = memNeeded(stack.get(stack.size() - 2), stack.get(stack.size() - 3));
                    break;
                case LOG0:
                case LOG1:
                case LOG2:
                case LOG3:
                case LOG4:

                    int nTopics = op.val() - OpCode.LOG0.val();
                    newMemSize = memNeeded(stack.peek(), stack.get(stack.size() - 2));

                    BigInteger dataSize = stack.get(stack.size() - 2).value();
                    BigInteger dataCost = dataSize.multiply(BigInteger.valueOf(GasCost.LOG_DATA_GAS));
                    if (program.getGas().value().compareTo(dataCost) < 0) {
                        throw program.new OutOfGasException();
                    }

                    gasCost = GasCost.LOG_GAS +
                            GasCost.LOG_TOPIC_GAS * nTopics +
                            GasCost.LOG_DATA_GAS * stack.get(stack.size() - 2).longValue();
                    break;
                case EXP:

                    DataWord exp = stack.get(stack.size() - 2);
                    int bytesOccupied = exp.bytesOccupied();
                    gasCost = GasCost.EXP_GAS + GasCost.EXP_BYTE_GAS * bytesOccupied;
                    break;
                default:
                    break;
            }
            program.spendGas(gasCost, op.name());

            // Avoid overflows
            if (newMemSize.compareTo(MAX_GAS) == 1)
                throw program.new OutOfGasException();

            // memory gas calc
            long memoryUsage = (newMemSize.longValue() + 31) / 32 * 32;
            if (memoryUsage > oldMemSize) {
                memWords = (memoryUsage - oldMemSize) / 32;
                long memGas = GasCost.MEMORY * (memWords + memWords * memWords / 1024);
                program.spendGas(memGas, op.name() + " (memory usage)");
                gasCost += memGas;
            }

            if (copySize > 0) {
                long copyGas = GasCost.COPY_GAS * (copySize + 31) / 32;
                gasCost += copyGas;
                program.spendGas(copyGas, op.name() + " (copy usage)");
            }

            // Log debugging line for VM
            if (program.getNumber().intValue() == CONFIG.dumpBlock())
                this.dumpLine(op, gasBefore, gasCost + callGas, memWords, program);

            // Execute operation
            switch (op) {
                /**
                 * Stop and Arithmetic Operations
                 */
                case STOP: {
                    program.setHReturn(ByteBuffer.allocate(0));
                    program.stop();
                }
                break;
                case ADD: {
                    DataWord word1 = program.stackPop();
                    DataWord word2 = program.stackPop();

                    if (logger.isInfoEnabled())
                        hint = word1.value() + " + " + word2.value();

                    word1.add(word2);
                    program.stackPush(word1);
                    program.step();

                }
                break;
                case MUL: {
                    DataWord word1 = program.stackPop();
                    DataWord word2 = program.stackPop();

                    if (logger.isInfoEnabled())
                        hint = word1.value() + " * " + word2.value();

                    word1.mul(word2);
                    program.stackPush(word1);
                    program.step();
                }
                break;
                case SUB: {
                    DataWord word1 = program.stackPop();
                    DataWord word2 = program.stackPop();

                    if (logger.isInfoEnabled())
                        hint = word1.value() + " - " + word2.value();

                    word1.sub(word2);
                    program.stackPush(word1);
                    program.step();
                }
                break;
                case DIV: {
                    DataWord word1 = program.stackPop();
                    DataWord word2 = program.stackPop();

                    if (logger.isInfoEnabled())
                        hint = word1.value() + " / " + word2.value();

                    word1.div(word2);
                    program.stackPush(word1);
                    program.step();
                }
                break;
                case SDIV: {
                    DataWord word1 = program.stackPop();
                    DataWord word2 = program.stackPop();

                    if (logger.isInfoEnabled())
                        hint = word1.sValue() + " / " + word2.sValue();

                    word1.sDiv(word2);
                    program.stackPush(word1);
                    program.step();
                }
                break;
                case MOD: {
                    DataWord word1 = program.stackPop();
                    DataWord word2 = program.stackPop();

                    if (logger.isInfoEnabled())
                        hint = word1.value() + " % " + word2.value();

                    word1.mod(word2);
                    program.stackPush(word1);
                    program.step();
                }
                break;
                case SMOD: {
                    DataWord word1 = program.stackPop();
                    DataWord word2 = program.stackPop();

                    if (logger.isInfoEnabled())
                        hint = word1.sValue() + " #% " + word2.sValue();

                    word1.sMod(word2);
                    program.stackPush(word1);
                    program.step();
                }
                break;
                case EXP: {
                    DataWord word1 = program.stackPop();
                    DataWord word2 = program.stackPop();

                    if (logger.isInfoEnabled())
                        hint = word1.value() + " ** " + word2.value();

                    word1.exp(word2);
                    program.stackPush(word1);
                    program.step();
                }
                break;
                case SIGNEXTEND: {
                    DataWord word1 = program.stackPop();
                    BigInteger k = word1.value();

                    if (k.compareTo(_32_) < 0) {
                        DataWord word2 = program.stackPop();
                        if (logger.isInfoEnabled())
                            hint = word1 + "  " + word2.value();
                        word2.signExtend(k.byteValue());
                        program.stackPush(word2);
                    }
                    program.step();
                }
                break;
                case NOT: {
                    DataWord word1 = program.stackPop();
                    word1.bnot();

                    if (logger.isInfoEnabled())
                        hint = "" + word1.value();

                    program.stackPush(word1);
                    program.step();
                }
                break;
                case LT: {
                    // TODO: can be improved by not using BigInteger
                    DataWord word1 = program.stackPop();
                    DataWord word2 = program.stackPop();

                    if (logger.isInfoEnabled())
                        hint = word1.value() + " < " + word2.value();

                    if (word1.value().compareTo(word2.value()) == -1) {
                        word1.and(DataWord.ZERO);
                        word1.getData()[31] = 1;
                    } else {
                        word1.and(DataWord.ZERO);
                    }
                    program.stackPush(word1);
                    program.step();
                }
                break;
                case SLT: {
                    // TODO: can be improved by not using BigInteger
                    DataWord word1 = program.stackPop();
                    DataWord word2 = program.stackPop();

                    if (logger.isInfoEnabled())
                        hint = word1.sValue() + " < " + word2.sValue();

                    if (word1.sValue().compareTo(word2.sValue()) == -1) {
                        word1.and(DataWord.ZERO);
                        word1.getData()[31] = 1;
                    } else {
                        word1.and(DataWord.ZERO);
                    }
                    program.stackPush(word1);
                    program.step();
                }
                break;
                case SGT: {
                    // TODO: can be improved by not using BigInteger
                    DataWord word1 = program.stackPop();
                    DataWord word2 = program.stackPop();

                    if (logger.isInfoEnabled())
                        hint = word1.sValue() + " > " + word2.sValue();

                    if (word1.sValue().compareTo(word2.sValue()) == 1) {
                        word1.and(DataWord.ZERO);
                        word1.getData()[31] = 1;
                    } else {
                        word1.and(DataWord.ZERO);
                    }
                    program.stackPush(word1);
                    program.step();
                }
                break;
                case GT: {
                    // TODO: can be improved by not using BigInteger
                    DataWord word1 = program.stackPop();
                    DataWord word2 = program.stackPop();

                    if (logger.isInfoEnabled())
                        hint = word1.value() + " > " + word2.value();

                    if (word1.value().compareTo(word2.value()) == 1) {
                        word1.and(DataWord.ZERO);
                        word1.getData()[31] = 1;
                    } else {
                        word1.and(DataWord.ZERO);
                    }
                    program.stackPush(word1);
                    program.step();
                }
                break;
                case EQ: {
                    DataWord word1 = program.stackPop();
                    DataWord word2 = program.stackPop();

                    if (logger.isInfoEnabled())
                        hint = word1.value() + " == " + word2.value();

                    if (word1.xor(word2).isZero()) {
                        word1.and(DataWord.ZERO);
                        word1.getData()[31] = 1;
                    } else {
                        word1.and(DataWord.ZERO);
                    }
                    program.stackPush(word1);
                    program.step();
                }
                break;
                case ISZERO: {
                    DataWord word1 = program.stackPop();
                    if (word1.isZero()) {
                        word1.getData()[31] = 1;
                    } else {
                        word1.and(DataWord.ZERO);
                    }

                    if (logger.isInfoEnabled())
                        hint = "" + word1.value();

                    program.stackPush(word1);
                    program.step();
                }
                break;

                /**
                 * Bitwise Logic Operations
                 */
                case AND: {
                    DataWord word1 = program.stackPop();
                    DataWord word2 = program.stackPop();

                    if (logger.isInfoEnabled())
                        hint = word1.value() + " && " + word2.value();

                    word1.and(word2);
                    program.stackPush(word1);
                    program.step();
                }
                break;
                case OR: {
                    DataWord word1 = program.stackPop();
                    DataWord word2 = program.stackPop();

                    if (logger.isInfoEnabled())
                        hint = word1.value() + " || " + word2.value();

                    word1.or(word2);
                    program.stackPush(word1);
                    program.step();
                }
                break;
                case XOR: {
                    DataWord word1 = program.stackPop();
                    DataWord word2 = program.stackPop();

                    if (logger.isInfoEnabled())
                        hint = word1.value() + " ^ " + word2.value();

                    word1.xor(word2);
                    program.stackPush(word1);
                    program.step();
                }
                break;
                case BYTE: {
                    DataWord word1 = program.stackPop();
                    DataWord word2 = program.stackPop();
                    final DataWord result;
                    if (word1.value().compareTo(_32_) == -1) {
                        byte tmp = word2.getData()[word1.intValue()];
                        word2.and(DataWord.ZERO);
                        word2.getData()[31] = tmp;
                        result = word2;
                    } else {
                        result = new DataWord();
                    }

                    if (logger.isInfoEnabled())
                        hint = "" + result.value();

                    program.stackPush(result);
                    program.step();
                }
                break;
                case ADDMOD: {
                    DataWord word1 = program.stackPop();
                    DataWord word2 = program.stackPop();
                    DataWord word3 = program.stackPop();
                    word1.addmod(word2, word3);
                    program.stackPush(word1);
                    program.step();
                }
                break;
                case MULMOD: {
                    DataWord word1 = program.stackPop();
                    DataWord word2 = program.stackPop();
                    DataWord word3 = program.stackPop();
                    word1.mulmod(word2, word3);
                    program.stackPush(word1);
                    program.step();
                }
                break;

                /**
                 * SHA3
                 */
                case SHA3: {
                    DataWord memOffsetData = program.stackPop();
                    DataWord lengthData = program.stackPop();
                    ByteBuffer buffer = program.memoryChunk(memOffsetData, lengthData);

                    byte[] encoded = HashUtil.sha3(buffer.array());
                    DataWord word = new DataWord(encoded);

                    if (logger.isInfoEnabled())
                        hint = word.toString();

                    program.stackPush(word);
                    program.step();
                }
                break;

                /**
                 * Environmental Information
                 */
                case ADDRESS: {
                    DataWord address = program.getOwnerAddress();

                    if (logger.isInfoEnabled())
                        hint = "address: " + Hex.toHexString(address.getLast20Bytes());

                    program.stackPush(address);
                    program.step();
                }
                break;
                case BALANCE: {
                    DataWord address = program.stackPop();
                    DataWord balance = program.getBalance(address);

                    if (logger.isInfoEnabled())
                        hint = "address: "
                                + Hex.toHexString(address.getLast20Bytes())
                                + " balance: " + balance.toString();

                    program.stackPush(balance);
                    program.step();
                }
                break;
                case ORIGIN: {
                    DataWord originAddress = program.getOriginAddress();

                    if (logger.isInfoEnabled())
                        hint = "address: " + Hex.toHexString(originAddress.getLast20Bytes());

                    program.stackPush(originAddress);
                    program.step();
                }
                break;
                case CALLER: {
                    DataWord callerAddress = program.getCallerAddress();

                    if (logger.isInfoEnabled())
                        hint = "address: " + Hex.toHexString(callerAddress.getLast20Bytes());

                    program.stackPush(callerAddress);
                    program.step();
                }
                break;
                case CALLVALUE: {
                    DataWord callValue = program.getCallValue();

                    if (logger.isInfoEnabled())
                        hint = "value: " + callValue;

                    program.stackPush(callValue);
                    program.step();
                }
                break;
                case CALLDATALOAD: {
                    DataWord dataOffs = program.stackPop();
                    DataWord value = program.getDataValue(dataOffs);

                    if (logger.isInfoEnabled())
                        hint = "data: " + value;

                    program.stackPush(value);
                    program.step();
                }
                break;
                case CALLDATASIZE: {
                    DataWord dataSize = program.getDataSize();

                    if (logger.isInfoEnabled())
                        hint = "size: " + dataSize.value();

                    program.stackPush(dataSize);
                    program.step();
                }
                break;
                case CALLDATACOPY: {
                    DataWord memOffsetData = program.stackPop();
                    DataWord dataOffsetData = program.stackPop();
                    DataWord lengthData = program.stackPop();

                    byte[] msgData = program.getDataCopy(dataOffsetData, lengthData);

                    if (logger.isInfoEnabled())
                        hint = "data: " + Hex.toHexString(msgData);

                    program.memorySave(memOffsetData.intValue(), msgData);
                    program.step();
                }
                break;
                case CODESIZE:
                case EXTCODESIZE: {

                    int length;
                    if (op == OpCode.CODESIZE)
                        length = program.getCode().length;
                    else {
                        DataWord address = program.stackPop();
                        length = program.getCodeAt(address).length;
                    }
                    DataWord codeLength = new DataWord(length);

                    if (logger.isInfoEnabled())
                        hint = "size: " + length;

                    program.stackPush(codeLength);
                    program.step();
                }
                break;
                case CODECOPY:
                case EXTCODECOPY: {

                    byte[] fullCode = ByteUtil.EMPTY_BYTE_ARRAY;
                    if (op == OpCode.CODECOPY)
                        fullCode = program.getCode();

                    if (op == OpCode.EXTCODECOPY) {
                        DataWord address = program.stackPop();
                        fullCode = program.getCodeAt(address);
                    }

                    int memOffset = program.stackPop().intValue();
                    int codeOffset = program.stackPop().intValue();
                    int lengthData = program.stackPop().intValue();

                    int sizeToBeCopied =
                            codeOffset + lengthData > fullCode.length ?
                                    (fullCode.length < codeOffset ? 0 : fullCode.length - codeOffset)
                                    : lengthData;

                    byte[] codeCopy = new byte[lengthData];

                    if (codeOffset < fullCode.length)
                        System.arraycopy(fullCode, codeOffset, codeCopy, 0, sizeToBeCopied);

                    if (logger.isInfoEnabled())
                        hint = "code: " + Hex.toHexString(codeCopy);

                    program.memorySave(memOffset, codeCopy);
                    program.step();
                }
                break;
                case GASPRICE: {
                    DataWord gasPrice = program.getGasPrice();

                    if (logger.isInfoEnabled())
                        hint = "price: " + gasPrice.toString();

                    program.stackPush(gasPrice);
                    program.step();
                }
                break;

                /**
                 * Block Information
                 */
                case BLOCKHASH: {

                    int blockIndex = program.stackPop().intValue();

                    DataWord blockHash = program.getBlockHash(blockIndex);

                    if (logger.isInfoEnabled())
                        hint = "blockHash: " + blockHash;

                    program.stackPush(blockHash);
                    program.step();
                }
                break;
                case COINBASE: {
                    DataWord coinbase = program.getCoinbase();

                    if (logger.isInfoEnabled())
                        hint = "coinbase: " + Hex.toHexString(coinbase.getLast20Bytes());

                    program.stackPush(coinbase);
                    program.step();
                }
                break;
                case TIMESTAMP: {
                    DataWord timestamp = program.getTimestamp();

                    if (logger.isInfoEnabled())
                        hint = "timestamp: " + timestamp.value();

                    program.stackPush(timestamp);
                    program.step();
                }
                break;
                case NUMBER: {
                    DataWord number = program.getNumber();

                    if (logger.isInfoEnabled())
                        hint = "number: " + number.value();

                    program.stackPush(number);
                    program.step();
                }
                break;
                case DIFFICULTY: {
                    DataWord difficulty = program.getDifficulty();

                    if (logger.isInfoEnabled())
                        hint = "difficulty: " + difficulty;

                    program.stackPush(difficulty);
                    program.step();
                }
                break;
                case GASLIMIT: {
                    DataWord gaslimit = program.getGaslimit();

                    if (logger.isInfoEnabled())
                        hint = "gaslimit: " + gaslimit;

                    program.stackPush(gaslimit);
                    program.step();
                }
                break;
                case POP: {
                    program.stackPop();
                    program.step();
                }   break;
                case DUP1: case DUP2: case DUP3: case DUP4:
                case DUP5: case DUP6: case DUP7: case DUP8:
                case DUP9: case DUP10: case DUP11: case DUP12:
                case DUP13: case DUP14: case DUP15: case DUP16:{

                    int n = op.val() - OpCode.DUP1.val() + 1;
                    DataWord word_1 = stack.get(stack.size() - n);
                    program.stackPush(word_1.clone());
                    program.step();

                }   break;
                case SWAP1: case SWAP2: case SWAP3: case SWAP4:
                case SWAP5: case SWAP6: case SWAP7: case SWAP8:
                case SWAP9: case SWAP10: case SWAP11: case SWAP12:
                case SWAP13: case SWAP14: case SWAP15: case SWAP16:{

                    int n = op.val() - OpCode.SWAP1.val() + 2;
                    DataWord word_1 = stack.peek();
                    stack.set(stack.size() - 1, stack.get(stack.size() - n));
                    stack.set(stack.size() - n, word_1);
                    program.step();

                }
                break;
                case LOG0:
                case LOG1:
                case LOG2:
                case LOG3:
                case LOG4: {

                    DataWord address = program.programAddress;

                    DataWord memStart = stack.pop();
                    DataWord memOffset = stack.pop();

                    int nTopics = op.val() - OpCode.LOG0.val();

                    List<DataWord> topics = new ArrayList<>();
                    for (int i = 0; i < nTopics; ++i) {
                        DataWord topic = stack.pop();
                        topics.add(topic);
                    }

                    ByteBuffer data = program.memoryChunk(memStart, memOffset);

                    LogInfo logInfo =
                            new LogInfo(address.getLast20Bytes(), topics, data.array());

                    if (logger.isInfoEnabled())
                        hint = logInfo.toString();

                    program.getResult().addLogInfo(logInfo);
                    program.step();
                }
                break;
                case MLOAD: {
                    DataWord addr = program.stackPop();
                    DataWord data = program.memoryLoad(addr);

                    if (logger.isInfoEnabled())
                        hint = "data: " + data;

                    program.stackPush(data);
                    program.step();
                }
                break;
                case MSTORE: {
                    DataWord addr = program.stackPop();
                    DataWord value = program.stackPop();

                    if (logger.isInfoEnabled())
                        hint = "addr: " + addr + " value: " + value;

                    program.memorySave(addr, value);
                    program.step();
                }
                break;
                case MSTORE8: {
                    DataWord addr = program.stackPop();
                    DataWord value = program.stackPop();
                    byte[] byteVal = {value.getData()[31]};
                    program.memorySave(addr.intValue(), byteVal);
                    program.step();
                }
                break;
                case SLOAD: {
                    DataWord key = program.stackPop();
                    DataWord val = program.storageLoad(key);

                    if (logger.isInfoEnabled())
                        hint = "key: " + key + " value: " + val;

                    if (val == null)
                        val = key.and(DataWord.ZERO);

                    program.stackPush(val);
                    program.step();
                }
                break;
                case SSTORE: {
                    DataWord addr = program.stackPop();
                    DataWord value = program.stackPop();

                    if (logger.isInfoEnabled())
                        hint = "addr: " + addr + " value: " + value;

                    program.storageSave(addr, value);
                    program.step();
                }
                break;
                case JUMP: {
                    DataWord pos = program.stackPop();
                    int nextPC = pos.intValue(); // possible overflow
                    program.validateJumpDest(nextPC);

                    if (logger.isInfoEnabled())
                        hint = "~> " + nextPC;

                    program.setPC(nextPC);

                }
                break;
                case JUMPI: {
                    DataWord pos = program.stackPop();
                    DataWord cond = program.stackPop();

                    if (!cond.isZero()) {

                        int nextPC = pos.intValue(); // possible overflow
                        program.validateJumpDest(nextPC);

                        if (logger.isInfoEnabled())
                            hint = "~> " + nextPC;

                        program.setPC(nextPC);
                    } else {
                        program.step();
                    }

                }
                break;
                case PC: {
                    int pc = program.getPC();
                    DataWord pcWord = new DataWord(pc);

                    if (logger.isInfoEnabled())
                        hint = pcWord.toString();

                    program.stackPush(pcWord);
                    program.step();
                }
                break;
                case MSIZE: {
                    int memSize = program.getMemSize();
                    DataWord wordMemSize = new DataWord(memSize);

                    if (logger.isInfoEnabled())
                        hint = "" + memSize;

                    program.stackPush(wordMemSize);
                    program.step();
                }
                break;
                case GAS: {
                    DataWord gas = program.getGas();

                    if (logger.isInfoEnabled())
                        hint = "" + gas;

                    program.stackPush(gas);
                    program.step();
                }
                break;

                case PUSH1:
                case PUSH2:
                case PUSH3:
                case PUSH4:
                case PUSH5:
                case PUSH6:
                case PUSH7:
                case PUSH8:
                case PUSH9:
                case PUSH10:
                case PUSH11:
                case PUSH12:
                case PUSH13:
                case PUSH14:
                case PUSH15:
                case PUSH16:
                case PUSH17:
                case PUSH18:
                case PUSH19:
                case PUSH20:
                case PUSH21:
                case PUSH22:
                case PUSH23:
                case PUSH24:
                case PUSH25:
                case PUSH26:
                case PUSH27:
                case PUSH28:
                case PUSH29:
                case PUSH30:
                case PUSH31:
                case PUSH32: {
                    program.step();
                    int nPush = op.val() - PUSH1.val() + 1;

                    byte[] data = program.sweep(nPush);

                    if (logger.isInfoEnabled())
                        hint = "" + Hex.toHexString(data);

                    program.stackPush(data);
                }
                break;
                case JUMPDEST: {
                    program.step();
                }
                break;
                case CREATE: {
                    DataWord value = program.stackPop();
                    DataWord inOffset = program.stackPop();
                    DataWord inSize = program.stackPop();

                    if (logger.isInfoEnabled())
                        logger.info(logString, program.getPC(),
                                String.format("%-12s", op.name()),
                                program.getGas().value(),
                                program.invokeData.getCallDeep(), hint);

                    program.createContract(value, inOffset, inSize);

                    program.step();
                }
                break;
                case CALL:
                case CALLCODE: {
                    DataWord gas = program.stackPop();
                    DataWord codeAddress = program.stackPop();
                    DataWord value = program.stackPop();

                    DataWord inDataOffs = program.stackPop();
                    DataWord inDataSize = program.stackPop();

                    DataWord outDataOffs = program.stackPop();
                    DataWord outDataSize = program.stackPop();

                    if (logger.isInfoEnabled()) {
                        hint = "addr: " + Hex.toHexString(codeAddress.getLast20Bytes())
                                + " gas: " + gas.shortHex()
                                + " inOff: " + inDataOffs.shortHex()
                                + " inSize: " + inDataSize.shortHex();
                        logger.info(logString, program.getPC(),
                                String.format("%-12s", op.name()),
                                program.getGas().value(),
                                program.invokeData.getCallDeep(), hint);
                    }

                    program.memoryExpand(outDataOffs, outDataSize);

                    MessageCall msg = new MessageCall(
                            op.equals(CALL) ? MsgType.CALL : MsgType.STATELESS,
                            gas, codeAddress, value, inDataOffs, inDataSize,
                            outDataOffs, outDataSize);

                    PrecompiledContracts.PrecompiledContract contract =
                            PrecompiledContracts.getContractForAddress(codeAddress);

                    if (contract != null)
                        program.callToPrecompiledAddress(msg, contract);
                    else
                        program.callToAddress(msg);

                    program.step();
                }
                break;
                case RETURN: {
                    DataWord offset = program.stackPop();
                    DataWord size = program.stackPop();

                    ByteBuffer hReturn = program.memoryChunk(offset, size);
                    program.setHReturn(hReturn);

                    if (logger.isInfoEnabled())
                        hint = "data: " + Hex.toHexString(hReturn.array())
                                + " offset: " + offset.value()
                                + " size: " + size.value();

                    program.step();
                    program.stop();
                }
                break;
                case SUICIDE: {
                    DataWord address = program.stackPop();
                    program.suicide(address);

                    if (logger.isInfoEnabled())
                        hint = "address: " + Hex.toHexString(program.getOwnerAddress().getLast20Bytes());

                    program.stop();
                }
                break;
                default:
                    break;
            }

            program.setPreviouslyExecutedOp(op.val());

            if (logger.isInfoEnabled() && !op.equals(CALL)
                    && !op.equals(CREATE))
                logger.info(logString, stepBefore, String.format("%-12s",
                                op.name()), program.getGas().longValue(),
                        program.invokeData.getCallDeep(), hint);

            vmCounter++;
        } catch (RuntimeException e) {
            logger.warn("VM halted: [{}]", e.toString());
            program.spendAllGas();
            program.stop();
            throw e;
        } finally {
            program.fullTrace();
        }
    }

    public void play(Program program) {
        try {
            // In case the program invoked by wire got
            // transaction, this will be the gas cost,
            // otherwise the call done by other contract
            // charged by CALL op
            if (program.invokeData.byTransaction()) {
                program.spendGas(GasCost.TRANSACTION, "TRANSACTION");
                int dataSize = program.invokeData.getDataSize().intValue();
                int nonZeroesVals = program.invokeData.countNonZeroData();
                int zeroVals = dataSize - nonZeroesVals;

                program.spendGas(GasCost.TX_NO_ZERO_DATA * nonZeroesVals, "DATA");
                program.spendGas(GasCost.TX_ZERO_DATA * zeroVals, "DATA");
            }

            if (program.invokeData.byTestingSuite()) return;

            while (!program.isStopped())
                this.step(program);

        } catch (RuntimeException e) {
            program.setRuntimeFailure(e);
        }
    }

    /**
     * Utility to calculate new total memory size needed for an operation.
     * <br/> Basically just offset + size, unless size is 0, in which case the result is also 0.
     *
     * @param offset starting position of the memory
     * @param size number of bytes needed
     * @return offset + size, unless size is 0. In that case memNeeded is also 0.
     */
    private BigInteger memNeeded(DataWord offset, DataWord size) {
        if (size.isZero())
            return BigInteger.ZERO;
        return offset.value().add(size.value());
    }

    /*
     * Dumping the VM state at the current operation in various styles
     *  - standard  Not Yet Implemented
     *  - standard+ (owner address, program counter, operation, gas left)
     *  - pretty (stack, memory, storage, level, contract,
     *              vmCounter, internalSteps, operation
                    gasBefore, gasCost, memWords)
     */
    private void dumpLine(OpCode op, long gasBefore, long gasCost, long memWords, Program program) {
        if (CONFIG.dumpStyle().equals("standard+")) {
            switch (op) {
                case STOP:
                case RETURN:
                case SUICIDE:

                    ContractDetails details = program.getResult().getRepository()
                            .getContractDetails(program.getOwnerAddress().getLast20Bytes());
                    List<DataWord> storageKeys = new ArrayList<>(details.getStorage().keySet());
                    Collections.sort(storageKeys);

                    for (DataWord key : storageKeys) {
                        dumpLogger.trace("{} {}",
                                Hex.toHexString(key.getNoLeadZeroesData()),
                                Hex.toHexString(details.getStorage().get(key).getNoLeadZeroesData()));
                    }
                default:
                    break;
            }
            String addressString = Hex.toHexString(program.getOwnerAddress().getLast20Bytes());
            String pcString = Hex.toHexString(new DataWord(program.getPC()).getNoLeadZeroesData());
            String opString = Hex.toHexString(new byte[]{op.val()});
            String gasString = Hex.toHexString(program.getGas().getNoLeadZeroesData());

            dumpLogger.trace("{} {} {} {}", addressString, pcString, opString, gasString);
        } else if (CONFIG.dumpStyle().equals("pretty")) {
            dumpLogger.trace("    STACK");
            for (DataWord item : program.getStack()) {
                dumpLogger.trace("{}", item);
            }
            dumpLogger.trace("    MEMORY");
            String memoryString = program.memoryToString();
            if (!"".equals(memoryString))
                dumpLogger.trace("{}", memoryString);

            dumpLogger.trace("    STORAGE");
            ContractDetails details = program.getResult().getRepository()
                    .getContractDetails(program.getOwnerAddress().getLast20Bytes());
            List<DataWord> storageKeys = new ArrayList<>(details.getStorage().keySet());
            Collections.sort(storageKeys);

            for (DataWord key : storageKeys) {
                dumpLogger.trace("{}: {}",
                        key.shortHex(),
                        details.getStorage().get(key).shortHex());
            }

            int level = program.invokeData.getCallDeep();
            String contract = Hex.toHexString(program.getOwnerAddress().getLast20Bytes());
            String internalSteps = String.format("%4s", Integer.toHexString(program.getPC())).replace(' ', '0').toUpperCase();
            dumpLogger.trace("{} | {} | #{} | {} : {} | {} | -{} | {}x32",
                    level, contract, vmCounter, internalSteps, op,
                    gasBefore, gasCost, memWords);
        }
    }
}
