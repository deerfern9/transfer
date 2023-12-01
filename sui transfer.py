import random
from pysui import handle_result
from pysui.sui.sui_bcs import bcs
from pysui.sui.sui_types import *
from pysui import SuiConfig, SyncClient
from pysui.abstracts import SignatureScheme
from pysui.sui.sui_txn import SyncTransaction
from pysui.sui.sui_types.address import SuiAddress
from pysui.sui.sui_clients.sync_client import SuiClient
from pysui.sui.sui_txresults.single_tx import SuiCoinObjects
from loguru import logger
import requests
import time
from pydantic import BaseModel
from typing import Optional

sui_rpc = 'https://sui-mainnet-rpc.nodereal.io'
value_to_leave_in_sui = (0.01, 0.03)


class Sui8192TransactionResult(BaseModel):
    address: str
    digest: str


class SuiTxResult(Sui8192TransactionResult):
    reason: Optional[str]


class SuiTx(BaseModel):
    builder: SyncTransaction
    gas: ObjectID
    merge_count: Optional[int]

    class Config:
        arbitrary_types_allowed = True


class SuiBalance(BaseModel):
    int: int
    float: float


class SuiTransferConfig(BaseModel):
    config: SuiConfig
    address: str

    class Config:
        arbitrary_types_allowed = True


def read_file(filename):
    result = []
    with open(filename, 'r') as file:
        for tmp in file.readlines():
            result.append(tmp.replace('\n', ''))

    return result


def write_to_file(filename, text):
    with open(filename, 'a') as file:
        file.write(f'{text}\n')


def generate_suins():
    word = requests.get('https://random-word-api.herokuapp.com/word').json()[0][:random.randint(7, 9)]
    while len(word) < random.randint(10, 14):
        word += str(random.randint(0, 9))

    return word


def get_all_token(client, token):
    while True:
        try:
            """ Возвращает все объекты адреса (если они есть) и их баланс """

            # Создаёт(если его нет) элемент "client"
            client = client if client else SuiClient(SuiConfig.default_config())
            # Достаёт все объекты указанного токена

            all_coin_type = client.get_coin(SuiString(token)).result_data.data

            # Обрабатывает объекты
            gas_objects: list[all_coin_type] = handle_result(
                client.get_gas(
                    client.config.active_address
                )
            ).data

            return all_coin_type

        except:
            time.sleep(5)


def get_sui_config(mnemonic: str) -> SuiConfig:
    sui_config = SuiConfig.user_config(rpc_url=sui_rpc)
    if '0x' in mnemonic:
        sui_config.add_keypair_from_keystring(keystring={
            'wallet_key': mnemonic,
            'key_scheme': SignatureScheme.ED25519
        })
    else:
        sui_config.recover_keypair_and_address(
            scheme=SignatureScheme.ED25519,
            mnemonics=mnemonic,
            derivation_path="m/44'/784'/0'/0'/0'"
        )
    sui_config.set_active_address(address=SuiAddress(sui_config.addresses[0]))

    return sui_config


def get_transfer_config(mnemonic_line: str) -> SuiTransferConfig:
    mnemonic = mnemonic_line.split(':')[0]
    address = mnemonic_line.split(':')[1]
    sui_config = SuiConfig.user_config(rpc_url=sui_rpc)
    if '0x' in mnemonic:
        sui_config.add_keypair_from_keystring(keystring={
            'wallet_key': mnemonic,
            'key_scheme': SignatureScheme.ED25519
        })
    else:
        sui_config.recover_keypair_and_address(
            scheme=SignatureScheme.ED25519,
            mnemonics=mnemonic,
            derivation_path="m/44'/784'/0'/0'/0'"
        )
    sui_config.set_active_address(address=SuiAddress(sui_config.addresses[0]))

    return SuiTransferConfig(config=sui_config, address=address)


def get_sui_coin_objects_for_merge(sui_config: SuiConfig, coin_type: SuiString = None):
    if coin_type:
        gas_coin_objects: SuiCoinObjects = handle_result(SuiClient(sui_config).get_coin(
            coin_type=coin_type,
            address=sui_config.active_address,
            fetch_all=True)
        )
    else:
        gas_coin_objects: SuiCoinObjects = handle_result(SuiClient(sui_config).get_gas(sui_config.active_address,
                                                                                       fetch_all=True))
    zero_coins = [x for x in gas_coin_objects.data if int(x.balance) == 0]
    non_zero_coins = [x for x in gas_coin_objects.data if int(x.balance) > 0]

    richest_coin = max(non_zero_coins, key=lambda x: int(x.balance), default=None)
    return zero_coins, non_zero_coins, richest_coin


def get_sui_coin_objects_for_merge_2(client):
    all_coin_type = get_all_token(client, "0x2::sui::SUI")

    gas_objects: list[all_coin_type] = handle_result(
        client.get_gas(
            client.config.active_address)
    ).data
    print(gas_objects)
    zero_coins = [x for x in gas_objects if int(x.balance) == 0]
    non_zero_coins = [x for x in gas_objects if int(x.balance) > 0]

    richest_coin = max(non_zero_coins, key=lambda x: int(x.balance), default=None)
    gas_amount_coin = min(non_zero_coins, key=lambda x: int(x.balance), default=None)

    if richest_coin:
        non_zero_coins.remove(richest_coin)

    return zero_coins, non_zero_coins, richest_coin, gas_amount_coin


def get_sui_balance(sui_config: SuiConfig, coin_type: SuiString = None, denomination: int = None) -> SuiBalance:
    tries = 0
    while True:
        tries += 1
        try:
            client = SuiClient(config=sui_config)
            if coin_type:
                coin_objects: SuiCoinObjects = client.get_coin(coin_type=coin_type, address=sui_config.active_address,
                                                               fetch_all=True).result_data
            else:
                coin_objects: SuiCoinObjects = client.get_gas(address=sui_config.active_address,
                                                              fetch_all=True).result_data

            balance = 0
            for obj in list(coin_objects.data):
                balance += int(obj.balance)

            if denomination:
                return SuiBalance(
                    int=balance,
                    float=round(balance / 10 ** denomination, 2)
                )
            else:
                return SuiBalance(
                    int=balance,
                    float=round(balance / 10 ** 9, 2)
                )
        except:
            if tries <= 5:
                time.sleep(3)
            else:
                return SuiBalance(
                    int=0,
                    float=0
                )


def get_sui_transfer_from_config(amount: float) -> SuiBalance:
    return SuiBalance(
        int=int(amount * 1_000_000_000),
        float=round(amount, 2)
    )


def get_balance_to_transfer(balance: SuiBalance, value_to_leave_in_sui: float) -> SuiBalance:
    value_to_leave_in_sui_int = int(value_to_leave_in_sui * 10 ** 9)
    balance_to_transfer_float = round(
        (balance.int - value_to_leave_in_sui_int) / 10 ** 9,
        random.randint(2, 4))
    balance_to_transfer_int = int(balance_to_transfer_float * 10 ** 9)

    return SuiBalance(
        int=balance_to_transfer_int,
        float=balance_to_transfer_float,
    )


def transaction_run(txb: SyncTransaction):
    """Example of simple executing a SuiTransaction."""
    # Set sender if not done already
    if not txb.signer_block.sender:
        txb.signer_block.sender = txb.client.config.active_address

    # Execute the transaction
    tx_result = txb.execute(gas_budget="1747880")
    if tx_result.is_ok():
        owner = tx_result.result_data.balance_changes[0]['owner']['AddressOwner']
        digest = tx_result.result_data.digest
        logger.success(f"{owner} | Transaction success! Digest: {digest}")
        write_to_file('Digests.txt', f'{owner};{digest}')
        return tx_result.result_data

    else:
        logger.error(f"Transaction error {tx_result}")


def create_gas_object(amount, client: SuiClient = None):
    client = client if client else SuiClient(SuiConfig.default_config())
    txer = SyncTransaction(client)

    amount = int(amount * 10 ** 9)
    spcoin = txer.split_coin(coin=bcs.Argument("GasCoin"), amounts=[amount])
    txer.transfer_objects(transfers=[spcoin], recipient=client.config.active_address)

    tx_result = txer.execute(gas_budget="1747880")

    if tx_result.is_ok():
        return logger.success("Create gas object done")
    else:
        return logger.error("Create gas object error")


def init_transaction(sui_config: SuiConfig, merge_gas_budget: bool = False) -> SyncTransaction:
    return SyncTransaction(
        client=SuiClient(sui_config),
        initial_sender=sui_config.active_address,
        merge_gas_budget=merge_gas_budget
    )


def init_transaction_2(client, merge_gas_budget: bool = False) -> SyncTransaction:
    return SyncTransaction(
        client=client,
        initial_sender=client.config.active_address,
        merge_gas_budget=merge_gas_budget)



def build_and_execute_tx(sui_config: SuiConfig,
                         transaction: SyncTransaction,
                         gas_object: ObjectID = None) -> SuiTxResult:
    # rpc_result = transaction.execute(use_gas_object=gas_object, gas_budget=SUI_DEFAULT_GAS_BUDGET)
    build = transaction.inspect_all()
    gas_used = build.effects.gas_used
    gas_budget = int((int(gas_used.computation_cost) + int(gas_used.non_refundable_storage_fee) +
                      abs(int(gas_used.storage_cost) - int(gas_used.storage_rebate))) * 1.1)

    if build.error:
        return SuiTxResult(
            address=str(sui_config.active_address),
            digest='',
            reason=build.error
        )
    else:
        try:
            if gas_object:
                rpc_result = transaction.execute(use_gas_object=gas_object, gas_budget=str(gas_budget))
            else:
                rpc_result = transaction.execute(gas_budget=str(gas_budget))
            if rpc_result.result_data:
                return rpc_result.result_data.digest
            else:
                return SuiTxResult(
                    address=str(sui_config.active_address),
                    digest='',
                    reason=str(rpc_result.result_string)
                )
        except Exception as e:
            logger.exception(e)


def merge_sui_coins_tx(client):
    merge_results = []
    zero_coins, non_zero_coins, richest_coin, _ = get_sui_coin_objects_for_merge_2(client)
    if len(zero_coins) and len(non_zero_coins):
        logger.info('Попытка to merge zero_coins.')
        transaction = init_transaction_2(client)
        transaction.merge_coins(merge_to=transaction.gas, merge_from=zero_coins)
        try:
            build_result = build_and_execute_tx(
                client,
                transaction=transaction,
                gas_object=ObjectID(richest_coin.object_id)
            )
        except:
            pass
        if build_result:
            merge_results.append(build_result)
            time.sleep(5)
        zero_coins, non_zero_coins, richest_coin, _ = get_sui_coin_objects_for_merge_2(client)

    if len(non_zero_coins):
        logger.info('Попытка to merge non_zero_coins.')
        transaction = init_transaction_2(client)
        transaction.merge_coins(merge_to=transaction.gas, merge_from=non_zero_coins)
        build_result = build_and_execute_tx(
            client,
            transaction=transaction,
            gas_object=ObjectID(richest_coin.object_id)
        )
        if build_result:
            merge_results.append(build_result)


def get_pre_merged_tx(sui_config: SuiConfig, transaction: SyncTransaction) -> SuiTx:
    merge_count = 0

    zero_coins, non_zero_coins, richest_coin = get_sui_coin_objects_for_merge(sui_config=sui_config)
    if len(zero_coins) and len(non_zero_coins):
        merge_count += 1
        transaction.merge_coins(merge_to=transaction.gas, merge_from=zero_coins)
    if len(non_zero_coins) > 1:
        non_zero_coins.remove(richest_coin)
        merge_count += 1
        transaction.merge_coins(merge_to=transaction.gas, merge_from=non_zero_coins)

    return SuiTx(builder=transaction, gas=ObjectID(richest_coin.object_id), merge_count=merge_count)


def transfer_sui_tx(sui_config: SuiConfig, recipient: str, amount: SuiBalance) -> SuiTxResult:
    tx_object = get_pre_merged_tx(sui_config=sui_config, transaction=init_transaction(sui_config=sui_config))
    transaction = tx_object.builder
    # transaction = init_transaction(sui_config=sui_config, merge_gas_budget=True)

    transaction.transfer_sui(
        recipient=SuiAddress(recipient),
        from_coin=transaction.gas,
        amount=amount.int,
    )

    return build_and_execute_tx(sui_config=sui_config, transaction=transaction, gas_object=tx_object.gas)


def main_transfer_executor(transfer_config: SuiTransferConfig, amount: str):
    sui_config = transfer_config.config
    recipient_address = transfer_config.address

    try:
        if amount.replace('.', '').isdigit():
            balance = get_sui_transfer_from_config(float(amount))
            balance_to_transfer = get_balance_to_transfer(balance=balance,
                                                          value_to_leave_in_sui=0)
        else:
            balance = get_sui_balance(sui_config=sui_config)
            value_to_leave_in_float = round(random.uniform(value_to_leave_in_sui[0], value_to_leave_in_sui[1]),
                                            random.randint(2, 4))
            balance_to_transfer = get_balance_to_transfer(balance=balance,
                                                          value_to_leave_in_sui=value_to_leave_in_float)

        result = transfer_sui_tx(sui_config=sui_config, recipient=recipient_address,
                                         amount=balance_to_transfer)
        logger.success(
                    f'{sui_config.active_address} -> {recipient_address} | '
                    f'transfer: {balance_to_transfer.float} $SUI | digest: {result}')

    except Exception as e:
        logger.exception(e)


def main():
    mnemonic_lines = read_file('mnemonics.txt')
    for mnemonic_line in mnemonic_lines:
        mnemonic, _, amount = mnemonic_line.split(':')
        transfer_config = get_transfer_config(mnemonic_line)
        sui_config = get_sui_config(mnemonic)
        client_ = SyncClient(sui_config)
        if len(get_all_token(client_, "0x2::sui::SUI")) not in [0, 1]:
            merge_sui_coins_tx(client_)
        time.sleep(0.1)
        main_transfer_executor(transfer_config, amount)


if __name__ == '__main__':
    main()
