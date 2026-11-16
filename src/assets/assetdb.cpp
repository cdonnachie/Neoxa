// Copyright (c) 2017-2019 The Raven Core developers
// Copyright (c) 2020-2021 The Neoxa Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include <util.h>
#include <consensus/params.h>
#include <script/ismine.h>
#include <tinyformat.h>
#include "assetdb.h"
#include "assets.h"
#include "validation.h"

#include <boost/thread.hpp>

static const char ASSET_FLAG = 'A';
static const char ASSET_ADDRESS_QUANTITY_FLAG = 'B';
static const char ADDRESS_ASSET_QUANTITY_FLAG = 'C';
static const char MY_ASSET_FLAG = 'M';
static const char BLOCK_ASSET_UNDO_DATA = 'U';
static const char MEMPOOL_REISSUED_TX = 'Z';

static size_t MAX_DATABASE_RESULTS = 50000;

CAssetsDB::CAssetsDB(size_t nCacheSize, bool fMemory, bool fWipe) : CDBWrapper(GetDataDir() / "assets", nCacheSize, fMemory, fWipe) {
}

bool CAssetsDB::WriteAssetData(const CNewAsset &asset, const int nHeight, const uint256& blockHash)
{
    CDatabasedAssetData data(asset, nHeight, blockHash);
    return Write(std::make_pair(ASSET_FLAG, asset.strName), data);
}

bool CAssetsDB::WriteAssetAddressQuantity(const std::string &assetName, const std::string &address, const CAmount &quantity)
{
    return Write(std::make_pair(ASSET_ADDRESS_QUANTITY_FLAG, std::make_pair(assetName, address)), quantity);
}

bool CAssetsDB::WriteAddressAssetQuantity(const std::string &address, const std::string &assetName, const CAmount& quantity) {
    return Write(std::make_pair(ADDRESS_ASSET_QUANTITY_FLAG, std::make_pair(address, assetName)), quantity);
}

bool CAssetsDB::ReadAssetData(const std::string& strName, CNewAsset& asset, int& nHeight, uint256& blockHash)
{

    CDatabasedAssetData data;
    bool ret =  Read(std::make_pair(ASSET_FLAG, strName), data);

    if (ret) {
        asset = data.asset;
        nHeight = data.nHeight;
        blockHash = data.blockHash;
    }

    return ret;
}

bool CAssetsDB::ReadAssetAddressQuantity(const std::string& assetName, const std::string& address, CAmount& quantity)
{
    return Read(std::make_pair(ASSET_ADDRESS_QUANTITY_FLAG, std::make_pair(assetName, address)), quantity);
}

bool CAssetsDB::ReadAddressAssetQuantity(const std::string &address, const std::string &assetName, CAmount& quantity) {
    return Read(std::make_pair(ADDRESS_ASSET_QUANTITY_FLAG, std::make_pair(address, assetName)), quantity);
}

bool CAssetsDB::EraseAssetData(const std::string& assetName)
{
    return Erase(std::make_pair(ASSET_FLAG, assetName));
}

bool CAssetsDB::EraseMyAssetData(const std::string& assetName)
{
    return Erase(std::make_pair(MY_ASSET_FLAG, assetName));
}

bool CAssetsDB::EraseAssetAddressQuantity(const std::string &assetName, const std::string &address) {
    return Erase(std::make_pair(ASSET_ADDRESS_QUANTITY_FLAG, std::make_pair(assetName, address)));
}

bool CAssetsDB::EraseAddressAssetQuantity(const std::string &address, const std::string &assetName) {
    return Erase(std::make_pair(ADDRESS_ASSET_QUANTITY_FLAG, std::make_pair(address, assetName)));
}

bool EraseAddressAssetQuantity(const std::string &address, const std::string &assetName);

bool CAssetsDB::EraseAllAssets()
{
    // Erase every ASSET_FLAG record, the asset metadata. -reindexassets
    // rebuilds these from the issuance and reissuance outputs in the blocks.
    CDBBatch batch(*this);
    size_t nErased = 0;

    std::unique_ptr<CDBIterator> pcursor(NewIterator());
    pcursor->Seek(std::make_pair(ASSET_FLAG, std::string()));

    while (pcursor->Valid()) {
        boost::this_thread::interruption_point();

        std::pair<char, std::string> key;
        if (!pcursor->GetKey(key) || key.first != ASSET_FLAG)
            break;

        batch.Erase(key);

        if (++nErased % 100000 == 0) {
            if (!WriteBatch(batch))
                return error("%s : failed to erase a batch of asset metadata", __func__);
            batch.Clear();
        }

        pcursor->Next();
    }

    if (!WriteBatch(batch, true))
        return error("%s : failed to erase the asset metadata", __func__);

    LogPrintf("%s : removed %u asset metadata records\n", __func__, nErased);
    return true;
}

bool CAssetsDB::EraseAllAddressQuantities()
{
    // ASSET_ADDRESS_QUANTITY_FLAG is keyed <asset, address> and
    // ADDRESS_ASSET_QUANTITY_FLAG is the same balance keyed <address, asset>.
    // Wipe the pair together: dropping only one of them would leave
    // listaddressesbyasset and listassetbalancesbyaddress disagreeing.
    const char flags[2] = {ASSET_ADDRESS_QUANTITY_FLAG, ADDRESS_ASSET_QUANTITY_FLAG};

    CDBBatch batch(*this);
    size_t nErased = 0;

    for (const char flag : flags) {
        std::unique_ptr<CDBIterator> pcursor(NewIterator());
        pcursor->Seek(std::make_pair(flag, std::make_pair(std::string(), std::string())));

        while (pcursor->Valid()) {
            boost::this_thread::interruption_point();

            std::pair<char, std::pair<std::string, std::string> > key;
            if (!pcursor->GetKey(key) || key.first != flag)
                break;

            batch.Erase(key);

            if (++nErased % 100000 == 0) {
                if (!WriteBatch(batch))
                    return error("%s : failed to erase a batch of the address index", __func__);
                batch.Clear();
            }

            pcursor->Next();
        }
    }

    if (!WriteBatch(batch, true))
        return error("%s : failed to erase the address index", __func__);

    LogPrintf("%s : removed %u address index entries\n", __func__, nErased);
    return true;
}

bool CAssetsDB::WriteAssetAddressQuantities(const std::map<std::pair<std::string, std::string>, CAmount>& balances)
{
    // Each balance is written under both orderings so the two directional
    // lookups stay in step.
    CDBBatch batch(*this);
    size_t nWritten = 0;

    for (const auto& balance : balances) {
        boost::this_thread::interruption_point();

        const std::string& assetName = balance.first.first;
        const std::string& address = balance.first.second;

        batch.Write(std::make_pair(ASSET_ADDRESS_QUANTITY_FLAG, std::make_pair(assetName, address)), balance.second);
        batch.Write(std::make_pair(ADDRESS_ASSET_QUANTITY_FLAG, std::make_pair(address, assetName)), balance.second);

        nWritten += 2;
        if (nWritten % 100000 == 0) {
            if (!WriteBatch(batch))
                return error("%s : failed to write a batch of the address index", __func__);
            batch.Clear();
        }
    }

    if (!WriteBatch(batch, true))
        return error("%s : failed to write the address index", __func__);

    LogPrintf("%s : wrote %u address index entries for %u balances\n", __func__, nWritten, balances.size());
    return true;
}

bool CAssetsDB::WriteBlockUndoAssetData(const uint256& blockhash, const std::vector<std::pair<std::string, CBlockAssetUndo> >& assetUndoData)
{
    return Write(std::make_pair(BLOCK_ASSET_UNDO_DATA, blockhash), assetUndoData);
}

bool CAssetsDB::ReadBlockUndoAssetData(const uint256 &blockhash, std::vector<std::pair<std::string, CBlockAssetUndo> > &assetUndoData)
{
    // If it exists, return the read value.
    if (Exists(std::make_pair(BLOCK_ASSET_UNDO_DATA, blockhash)))
           return Read(std::make_pair(BLOCK_ASSET_UNDO_DATA, blockhash), assetUndoData);

    // If it doesn't exist, we just return true because we don't want to fail just because it didn't exist in the db
    return true;
}

bool CAssetsDB::WriteReissuedMempoolState()
{
    return Write(MEMPOOL_REISSUED_TX, mapReissuedAssets);
}

bool CAssetsDB::ReadReissuedMempoolState()
{
    mapReissuedAssets.clear();
    mapReissuedTx.clear();
    // If it exists, return the read value.
    bool rv = Read(MEMPOOL_REISSUED_TX, mapReissuedAssets);
    if (rv) {
        for (auto pair : mapReissuedAssets)
            mapReissuedTx.insert(std::make_pair(pair.second, pair.first));
    }
    return rv;
}

bool CAssetsDB::LoadAssets()
{
    std::unique_ptr<CDBIterator> pcursor(NewIterator());

    pcursor->Seek(std::make_pair(ASSET_FLAG, std::string()));

    // Load assets
    while (pcursor->Valid()) {
        boost::this_thread::interruption_point();
        std::pair<char, std::string> key;
        if (pcursor->GetKey(key) && key.first == ASSET_FLAG) {
            CDatabasedAssetData data;
            if (pcursor->GetValue(data)) {
                passetsCache->Put(data.asset.strName, data);
                pcursor->Next();

                // Loaded enough from database to have in memory.
                // No need to load everything if it is just going to be removed from the cache
                if (passetsCache->Size() == (passetsCache->MaxSize() / 2))
                    break;
            } else {
                return error("%s: failed to read asset", __func__);
            }
        } else {
            break;
        }
    }


    if (fAssetIndex) {
        std::unique_ptr<CDBIterator> pcursor3(NewIterator());
        pcursor3->Seek(std::make_pair(ASSET_ADDRESS_QUANTITY_FLAG, std::make_pair(std::string(), std::string())));

        // Load mapAssetAddressAmount
        while (pcursor3->Valid()) {
            boost::this_thread::interruption_point();
            std::pair<char, std::pair<std::string, std::string> > key; // <Asset Name, Address> -> Quantity
            if (pcursor3->GetKey(key) && key.first == ASSET_ADDRESS_QUANTITY_FLAG) {
                CAmount value;
                if (pcursor3->GetValue(value)) {
                    passets->mapAssetsAddressAmount.insert(
                            std::make_pair(std::make_pair(key.second.first, key.second.second), value));
                    if (passets->mapAssetsAddressAmount.size() > MAX_CACHE_ASSETS_SIZE)
                        break;
                    pcursor3->Next();
                } else {
                    return error("%s: failed to read my address quantity from database", __func__);
                }
            } else {
                break;
            }
        }
    }

    return true;
}

bool CAssetsDB::AssetDir(std::vector<CDatabasedAssetData>& assets, const std::string filter, const size_t count, const long start)
{
    FlushStateToDisk();

    std::unique_ptr<CDBIterator> pcursor(NewIterator());
    pcursor->Seek(std::make_pair(ASSET_FLAG, std::string()));

    auto prefix = filter;
    bool wildcard = prefix.back() == '*';
    if (wildcard)
        prefix.pop_back();

    size_t skip = 0;
    if (start >= 0) {
        skip = start;
    }
    else {
        // compute table size for backwards offset
        long table_size = 0;
        while (pcursor->Valid()) {
            boost::this_thread::interruption_point();

            std::pair<char, std::string> key;
            if (pcursor->GetKey(key) && key.first == ASSET_FLAG) {
                if (prefix == "" ||
                    (wildcard && key.second.find(prefix) == 0) ||
                    (!wildcard && key.second == prefix)) {
                    table_size += 1;
                }
            }
            pcursor->Next();
        }
        skip = table_size + start;
        pcursor->SeekToFirst();
    }


    size_t loaded = 0;
    size_t offset = 0;

    // Load assets
    while (pcursor->Valid() && loaded < count) {
        boost::this_thread::interruption_point();

        std::pair<char, std::string> key;
        if (pcursor->GetKey(key) && key.first == ASSET_FLAG) {
            if (prefix == "" ||
                    (wildcard && key.second.find(prefix) == 0) ||
                    (!wildcard && key.second == prefix)) {
                if (offset < skip) {
                    offset += 1;
                }
                else {
                    CDatabasedAssetData data;
                    if (pcursor->GetValue(data)) {
                        assets.push_back(data);
                        loaded += 1;
                    } else {
                        return error("%s: failed to read asset", __func__);
                    }
                }
            }
            pcursor->Next();
        } else {
            break;
        }
    }

    return true;
}

bool CAssetsDB::AddressDir(std::vector<std::pair<std::string, CAmount> >& vecAssetAmount, int& totalEntries, const bool& fGetTotal, const std::string& address, const size_t count, const long start)
{
    FlushStateToDisk();

    std::unique_ptr<CDBIterator> pcursor(NewIterator());
    pcursor->Seek(std::make_pair(ADDRESS_ASSET_QUANTITY_FLAG, std::make_pair(address, std::string())));

    if (fGetTotal) {
        totalEntries = 0;
        while (pcursor->Valid()) {
            boost::this_thread::interruption_point();

            std::pair<char, std::pair<std::string, std::string> > key;
            if (pcursor->GetKey(key) && key.first == ADDRESS_ASSET_QUANTITY_FLAG && key.second.first == address) {
                totalEntries++;
            }
            pcursor->Next();
        }
        return true;
    }

    size_t skip = 0;
    if (start >= 0) {
        skip = start;
    }
    else {
        // compute table size for backwards offset
        long table_size = 0;
        while (pcursor->Valid()) {
            boost::this_thread::interruption_point();

            std::pair<char, std::pair<std::string, std::string> > key;
            if (pcursor->GetKey(key) && key.first == ADDRESS_ASSET_QUANTITY_FLAG && key.second.first == address) {
                table_size += 1;
            }
            pcursor->Next();
        }
        skip = table_size + start;
        pcursor->SeekToFirst();
    }


    size_t loaded = 0;
    size_t offset = 0;

    // Load assets
    while (pcursor->Valid() && loaded < count && loaded < MAX_DATABASE_RESULTS) {
        boost::this_thread::interruption_point();

        std::pair<char, std::pair<std::string, std::string> > key;
        if (pcursor->GetKey(key) && key.first == ADDRESS_ASSET_QUANTITY_FLAG && key.second.first == address) {
                if (offset < skip) {
                    offset += 1;
                }
                else {
                    CAmount amount;
                    if (pcursor->GetValue(amount)) {
                        vecAssetAmount.emplace_back(std::make_pair(key.second.second, amount));
                        loaded += 1;
                    } else {
                        return error("%s: failed to Address Asset Quanity", __func__);
                    }
                }
            pcursor->Next();
        } else {
            break;
        }
    }

    return true;
}

// Can get to total count of addresses that belong to a certain asset_name, or get you the list of all address that belong to a certain asset_name
bool CAssetsDB::AssetAddressDir(std::vector<std::pair<std::string, CAmount> >& vecAddressAmount, int& totalEntries, const bool& fGetTotal, const std::string& assetName, const size_t count, const long start)
{
    FlushStateToDisk();

    std::unique_ptr<CDBIterator> pcursor(NewIterator());
    pcursor->Seek(std::make_pair(ASSET_ADDRESS_QUANTITY_FLAG, std::make_pair(assetName, std::string())));

    if (fGetTotal) {
        totalEntries = 0;
        while (pcursor->Valid()) {
            boost::this_thread::interruption_point();

            std::pair<char, std::pair<std::string, std::string> > key;
            if (pcursor->GetKey(key) && key.first == ASSET_ADDRESS_QUANTITY_FLAG && key.second.first == assetName) {
                totalEntries += 1;
            }
            pcursor->Next();
        }
        return true;
    }

    size_t skip = 0;
    if (start >= 0) {
        skip = start;
    }
    else {
        // compute table size for backwards offset
        long table_size = 0;
        while (pcursor->Valid()) {
            boost::this_thread::interruption_point();

            std::pair<char, std::pair<std::string, std::string> > key;
            if (pcursor->GetKey(key) && key.first == ASSET_ADDRESS_QUANTITY_FLAG && key.second.first == assetName) {
                table_size += 1;
            }
            pcursor->Next();
        }
        skip = table_size + start;
        pcursor->SeekToFirst();
    }

    size_t loaded = 0;
    size_t offset = 0;

    // Load assets
    while (pcursor->Valid() && loaded < count && loaded < MAX_DATABASE_RESULTS) {
        boost::this_thread::interruption_point();

        std::pair<char, std::pair<std::string, std::string> > key;
        if (pcursor->GetKey(key) && key.first == ASSET_ADDRESS_QUANTITY_FLAG && key.second.first == assetName) {
            if (offset < skip) {
                offset += 1;
            }
            else {
                CAmount amount;
                if (pcursor->GetValue(amount)) {
                    vecAddressAmount.emplace_back(std::make_pair(key.second.second, amount));
                    loaded += 1;
                } else {
                    return error("%s: failed to Asset Address Quanity", __func__);
                }
            }
            pcursor->Next();
        } else {
            break;
        }
    }

    return true;
}

bool CAssetsDB::AssetDir(std::vector<CDatabasedAssetData>& assets)
{
    return CAssetsDB::AssetDir(assets, "*", MAX_SIZE, 0);
}
