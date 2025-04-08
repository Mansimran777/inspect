const { MongoClient } = require('mongodb');
const winston = require('winston');
const utils = require('./utils');

class MongoDB {
    constructor(url, enableBulkInserts) {
        this.url = url;
        this.enableBulkInserts = enableBulkInserts || false;
        this.client = null;
        this.db = null;

        if (enableBulkInserts) {
            this.queuedInserts = [];

            setInterval(() => {
                if (this.queuedInserts.length > 0) {
                    const copy = [...this.queuedInserts];
                    this.queuedInserts = [];
                    this.handleBulkInsert(copy);
                }
            }, 1000);
        }
    }

    async connect() {
        try {
            const options = {
                useNewUrlParser: true,
                useUnifiedTopology: true,
                retryWrites: true,
                w: 'majority',
                connectTimeoutMS: 30000,
                socketTimeoutMS: 30000,
                serverSelectionTimeoutMS: 30000,
            };

            this.client = await MongoClient.connect(this.url, options);
            this.db = this.client.db('skinportaldb');
            
            await this.ensureSchema();
            winston.info('Connected to MongoDB successfully');
        } catch (err) {
            winston.error('Failed to connect to MongoDB:', err);
            throw err;
        }
    }

    async ensureSchema() {
        try {
            // Create collections if they don't exist
            const collections = await this.db.listCollections().toArray();
            const collectionNames = collections.map(col => col.name);
            if (!collectionNames.includes('items')) {
                await this.db.createCollection('items');
            }
            if (!collectionNames.includes('history')) {
                await this.db.createCollection('history');
            }

            // Create indexes
            await this.db.collection('items').createIndex(
                { defindex: 1, paintindex: 1, paintwear: 1, paintseed: 1 }, 
                { unique: true }
            );
            await this.db.collection('items').createIndex(
                { floatid: 1 }, 
                { unique: true }
            );
            await this.db.collection('items').createIndex(
                { 'stickers.i': 1 }, 
                { sparse: true }
            );
            await this.db.collection('items').createIndex(
                { paintwear: 1 }
            );
            await this.db.collection('items').createIndex(
                { floatvalue: 1 }
            ); // Added floatvalue index

            await this.db.collection('items').createIndex(
                { listed_price: 1 },
                { sparse: true }
            ); // Added listed_price index if necessary

            winston.info('MongoDB schema and indexes created successfully');
        } catch (err) {
            winston.error('Error creating MongoDB schema:', err);
            throw err;
        }
    }

    static storeProperties(origin, quality, rarity) {
        return {
            origin,
            quality,
            rarity
        };
    }

    async insertItemData(item, price) {
        if (this.enableBulkInserts) {
            this.queuedInserts.push([item, price]);
        } else {
            await this.handleBulkInsert([[item, price]]);
        }
    }

    /**
     * Bulk handler to improve insert performance with 300+ rows at once
     * @param data [[item, price]]
     * @returns {Promise<void>}
     */
    async handleBulkInsert(data) {
        const bulkOps = [];
        const uniqueItems = new Set();

        for (let [item, price] of data) {
            item = Object.assign({}, item);

            // Convert float to buffer to prevent rounding errors
            const buf = Buffer.alloc(4);
            buf.writeFloatBE(item.floatvalue, 0);
            item.paintwear = buf.readInt32BE(0);

            if (item.floatvalue <= 0 && item.defindex !== 507) {
                // Only insert weapons, naive check
                // Special case for the 0 float Karambit
                continue;
            }

            // Convert unsigned 64-bit to signed
            item.s = utils.unsigned64ToSigned(item.s).toString();
            item.a = utils.unsigned64ToSigned(item.a).toString();
            item.d = utils.unsigned64ToSigned(item.d).toString();
            item.m = utils.unsigned64ToSigned(item.m).toString();

            // Process stickers with additional fields
            const stickers = item.stickers && item.stickers.length > 0 ? item.stickers.map((s) => {
                const res = { 
                    s: s.slot, 
                    i: s.stickerId,
                    codename: s.codename || null,
                    material: s.material || null,
                    name: s.name || null
                };
                if (s.wear) res.w = s.wear;
                if (s.rotation) res.r = s.rotation;
                if (s.offset_x) res.x = s.offset_x;
                if (s.offset_y) res.y = s.offset_y;
                return res;
            }) : null;

            if (stickers) {
                for (const sticker of stickers) {
                    const matching = stickers.filter((s) => s.i === sticker.i);
                    if (matching.length > 1 && !matching.find((s) => s.d > 1)) {
                        sticker.d = matching.length;
                    }
                }
            }

            const ms = item.s !== '0' ? item.s : item.m;
            const isStattrak = item.stattrak || false;
            const isSouvenir = item.souvenir || false;

            const props = MongoDB.storeProperties(item.origin, item.quality, item.rarity);

            // Prevent duplicates in bulk insert based on unique key
            const key = `${item.defindex}_${item.paintindex}_${item.paintwear}_${item.paintseed}`;
            if (uniqueItems.has(key)) continue;
            uniqueItems.add(key);

            const floatid = item.floatid || item.a;

            const doc = {
                ms,
                a: item.a,
                d: item.d,
                paintseed: item.paintseed,
                paintwear: item.paintwear,
                defindex: item.defindex,
                paintindex: item.paintindex,
                stattrak: isStattrak,
                souvenir: isSouvenir,
                props,
                stickers,
                updated: new Date(),
                rarity: item.rarity,
                floatid,
                price: price || null,
                listed_price: item.listed_price || null, // Ensure listed_price is included
                floatvalue: item.floatvalue, // Include floatvalue directly
                inventory: item.inventory || null, // Include inventory if available
                imageurl: item.imageurl || null, // Include imageurl if available
                weapon_type: item.weapon_type || null, // Include weapon_type if available
                item_name: item.item_name || null, // Include item_name if available
                rarity_name: item.rarity_name || null, // Include rarity_name if available
                quality_name: item.quality_name || null, // Include quality_name if available
                origin_name: item.origin_name || null, // Include origin_name if available
                wear_name: item.wear_name || null, // Include wear_name if available
                full_item_name: item.full_item_name || null // Include full_item_name if available
            };

            // Store current state in history if needed
            if (utils.isSteamId64(item.s) || price) {
                await this.addToHistory({
                    floatid,
                    a: item.a,
                    steamid: utils.isSteamId64(item.s) ? item.s : null,
                    price
                });
            }

            bulkOps.push({
                updateOne: {
                    filter: {
                        defindex: item.defindex,
                        paintindex: item.paintindex,
                        paintwear: item.paintwear,
                        paintseed: item.paintseed
                    },
                    update: {
                        $set: doc
                    },
                    upsert: true
                }
            });
        }

        if (bulkOps.length === 0) return;

        try {
            const result = await this.db.collection('items').bulkWrite(bulkOps);
            winston.debug(`Inserted/updated ${result.upsertedCount + result.modifiedCount} items`);
        } catch (err) {
            winston.warn(err);
        }
    }

    async updateItemPrice(assetId, price) {
        const item = await this.db.collection('items').findOne({ a: assetId });
        if (!item) return;

        // Add price update to history
        await this.addToHistory({
            floatid: item.floatid,
            a: assetId,
            steamid: null,
            price
        });

        return this.db.collection('items').updateOne(
            { a: assetId },
            { $set: { price } }
        );
    }

    async getItemRank(id) {
        const item = await this.db.collection('items').findOne({ a: id });
        if (!item) return {};

        // Get low rank (items with lower paintwear)
        const lowRankCount = await this.db.collection('items').countDocuments({
            defindex: item.defindex,
            paintindex: item.paintindex,
            paintwear: { $lt: item.paintwear },
            stattrak: item.stattrak,
            souvenir: item.souvenir
        });

        // Get high rank (items with higher paintwear)
        const highRankCount = await this.db.collection('items').countDocuments({
            defindex: item.defindex,
            paintindex: item.paintindex,
            paintwear: { $gt: item.paintwear },
            stattrak: item.stattrak,
            souvenir: item.souvenir
        });

        const result = {};

        // Only include ranks if they're within the first 1000
        if (highRankCount < 1000) result.high_rank = highRankCount + 1;
        if (lowRankCount < 1000) result.low_rank = lowRankCount + 1;

        return result;
    }

    async getItemData(links) {
        const aValues = links.map(e => utils.unsigned64ToSigned(e.getParams().a));
        const items = await this.db.collection('items')
            .find({ a: { $in: aValues } })
            .toArray();

        return items.map(item => {
            // Convert properties back to match API format
            if (item.stattrak) {
                item.killeatervalue = 0;
            } else {
                item.killeatervalue = null;
            }

            item.stickers = item.stickers || [];
            item.stickers = item.stickers.map(s => ({
                slot: s.s,
                stickerId: s.i,
                codename: s.codename || null,
                material: s.material || null,
                name: s.name || null,
                wear: s.w || null,
                rotation: s.r || null,
                offset_x: s.x || null,
                offset_y: s.y || null,
                dupe_count: s.d || null
            }));

            // Floatvalue is stored directly
            // Ensure floatvalue is present
            item.floatvalue = item.floatvalue || 0.0;

            item.a = utils.signed64ToUnsigned(item.a).toString();
            item.d = utils.signed64ToUnsigned(item.d).toString();
            item.ms = utils.signed64ToUnsigned(item.ms).toString();

            if (utils.isSteamId64(item.ms)) {
                item.s = item.ms;
                item.m = '0';
            } else {
                item.m = item.ms;
                item.s = '0';
            }

            // Add properties from props object
            if (item.props) {
                item.origin = item.props.origin;
                item.quality = item.props.quality;
                item.rarity = item.props.rarity;
            }

            // Include additional fields
            item.inventory = item.inventory || null;
            item.imageurl = item.imageurl || null;
            item.weapon_type = item.weapon_type || null;
            item.item_name = item.item_name || null;
            item.rarity_name = item.rarity_name || null;
            item.quality_name = item.quality_name || null;
            item.origin_name = item.origin_name || null;
            item.wear_name = item.wear_name || null;
            item.full_item_name = item.full_item_name || null;

            // Clean up unwanted properties
            delete item.souvenir;
            delete item.stattrak;
            delete item.paintwear;
            delete item.ms;
            delete item.props;
            delete item.price;
            delete item.listed_price;
            delete item.dupe_count;
            delete item._id;
            delete item.updated;

            return item;
        });
    }

    async addToHistory(item) {
        const historyDoc = {
            floatid: item.floatid,
            a: item.a,
            steamid: item.steamid,
            created_at: new Date(),
            price: item.price
        };

        await this.db.collection('history').insertOne(historyDoc);
    }

    async close() {
        if (this.client) {
            await this.client.close();
            this.client = null;
            this.db = null;
        }
    }
}

module.exports = MongoDB;
