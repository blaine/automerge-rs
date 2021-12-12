
const assert = require('assert')
const util = require('util')
const Automerge = require('..')
const { MAP, LIST, TEXT } = Automerge

describe('Automerge', () => {
  describe('basics', () => {
    it('should init clone and free', () => {
      let doc1 = Automerge.init()
      let doc2 = doc1.clone()
      doc1.free()
      doc2.free()
    })

    it('should be able to start and commit', () => {
      let doc = Automerge.init()
      doc.begin()
      doc.commit()
    })

    it('calling begin inside a transaction should throw an error', () => {
      let doc = Automerge.init()
      doc.begin()
      assert.throws(() => { doc.begin() }, Error);
    })

    it('calling commit outside a transaction should throw an error', () => {
      let doc = Automerge.init()
      assert.throws(() => { doc.commit() }, Error);
    })

    it('getting a nonexistant prop does not throw an error', () => {
      let doc = Automerge.init()
      let root = "_root"
      let result = doc.value(root,"hello")
      assert.deepEqual(result,[])
    })

    it('should be able to set and get a simple value', () => {
      let doc = Automerge.init()
      let root = "_root"
      let result

      doc.begin()
      doc.set(root, "hello", "world")
      doc.set(root, "number", 5, "uint")
      doc.commit()

      result = doc.value(root,"hello")
      assert.deepEqual(result,["str","world"])

      result = doc.value(root,"number")
      assert.deepEqual(result,["uint",5])
    })

    it('should be able to use bytes', () => {
      let doc = Automerge.init()
      doc.begin()
      doc.set("_root","data", new Uint8Array([10,11,12]));
      doc.commit()
      let value = doc.value("_root", "data")
      assert.deepEqual(value, ["bytes", new Uint8Array([10,11,12])]);
    })

    it('should be able to make sub objects', () => {
      let doc = Automerge.init()
      let root = "_root"
      let result

      doc.begin()
      let submap = doc.set(root, "submap", MAP)
      doc.set(submap, "number", 6, "uint")
      assert.strictEqual(doc.pending_ops(),2)
      doc.commit()

      result = doc.value(root,"submap")
      assert.deepEqual(result,["map",submap])

      result = doc.value(submap,"number")
      assert.deepEqual(result,["uint",6])
    })

    it('should be able to make lists', () => {
      let doc = Automerge.init()
      let root = "_root"

      doc.begin()
      let submap = doc.set(root, "numbers", LIST)
      doc.insert(submap, 0, "a");
      doc.insert(submap, 1, "b");
      doc.insert(submap, 2, "c");
      doc.insert(submap, 0, "z");
      doc.commit()

      assert.deepEqual(doc.value(submap, 0),["str","z"])
      assert.deepEqual(doc.value(submap, 1),["str","a"])
      assert.deepEqual(doc.value(submap, 2),["str","b"])
      assert.deepEqual(doc.value(submap, 3),["str","c"])
      assert.deepEqual(doc.length(submap),4)

      doc.begin()
      doc.set(submap, 2, "b v2");
      doc.commit()

      assert.deepEqual(doc.value(submap, 2),["str","b v2"])
      assert.deepEqual(doc.length(submap),4)
    })

    it('should be able delete non-existant props', () => {
      let doc = Automerge.init()

      doc.begin()
      doc.set("_root", "foo","bar")
      doc.set("_root", "bip","bap")
      doc.commit()

      assert.deepEqual(doc.keys("_root"),["bip","foo"])

      doc.begin()
      doc.del("_root", "foo")
      doc.del("_root", "baz")
      doc.commit()

      assert.deepEqual(doc.keys("_root"),["bip"])
    })

    it('should be able to del', () => {
      let doc = Automerge.init()
      let root = "_root"

      doc.begin()
      doc.set(root, "xxx", "xxx");
      assert.deepEqual(doc.value(root, "xxx"),["str","xxx"])
      doc.del(root, "xxx");
      assert.deepEqual(doc.value(root, "xxx"),[])
      doc.commit()
    })

    it('should be able to use counters', () => {
      let doc = Automerge.init()
      let root = "_root"

      doc.begin()
      doc.set(root, "counter", 10, "counter");
      assert.deepEqual(doc.value(root, "counter"),["counter",10])
      doc.inc(root, "counter", 10);
      assert.deepEqual(doc.value(root, "counter"),["counter",20])
      doc.inc(root, "counter", -5);
      assert.deepEqual(doc.value(root, "counter"),["counter",15])
      doc.commit()
    })

    it('should be able to splice text', () => {
      let doc = Automerge.init()
      let root = "_root";

      doc.begin()
      let text = doc.set(root, "text", Automerge.TEXT);
      doc.splice(text, 0, 0, "hello ")
      doc.splice(text, 6, 0, ["w","o","r","l","d"])
      doc.splice(text, 11, 0, [["str","!"],["str","?"]])
      assert.deepEqual(doc.value(text, 0),["str","h"])
      assert.deepEqual(doc.value(text, 1),["str","e"])
      assert.deepEqual(doc.value(text, 9),["str","l"])
      assert.deepEqual(doc.value(text, 10),["str","d"])
      assert.deepEqual(doc.value(text, 11),["str","!"])
      assert.deepEqual(doc.value(text, 12),["str","?"])
      doc.commit()
    })

    it('should be able save all or incrementally', () => {
      let doc = Automerge.init()

      doc.begin()
      doc.set("_root", "foo", 1)
      doc.commit()

      let save1 = doc.save()

      doc.begin()
      doc.set("_root", "bar", 2)
      doc.commit()

      let save2 = doc.save_incremental()

      doc.begin()
      doc.set("_root", "baz", 3)
      doc.commit()

      let save3 = doc.save_incremental()

      let saveA = doc.save();
      let saveB = new Uint8Array([... save1, ...save2, ...save3]);

      assert.notDeepEqual(saveA, saveB);

      let docA = Automerge.load(saveA);
      let docB = Automerge.load(saveB);

      assert.deepEqual(docA.keys("_root"), docB.keys("_root"));
      assert.deepEqual(docA.save(), docB.save());
    })

    it('should be able to splice text', () => {
      let doc = Automerge.init()
      doc.begin();
      let text = doc.set("_root", "text", TEXT);
      doc.splice(text, 0, 0, "hello world");
      doc.splice(text, 6, 0, "big bad ");
      doc.commit()
      assert.strictEqual(doc.text(text), "hello big bad world")
    })

    it('local inc increments all visible counters in a map', () => {
      let doc1 = Automerge.init("aaaa")
      doc1.begin()
      doc1.set("_root", "hello", "world")
      doc1.commit()
      let doc2 = Automerge.load(doc1.save(), "bbbb");
      let doc3 = Automerge.load(doc1.save(), "cccc");
      doc1.begin()
      doc1.set("_root", "cnt", 20)
      doc1.commit()
      doc2.begin()
      doc2.set("_root", "cnt", 0, "counter")
      doc2.commit()
      doc3.begin()
      doc3.set("_root", "cnt", 10, "counter")
      doc3.commit()
      doc1.applyChanges(doc2.getChanges(doc1.getHeads()))
      doc1.applyChanges(doc3.getChanges(doc1.getHeads()))
      let result = doc1.values("_root", "cnt")
      assert.deepEqual(result,[
        ['counter',10,'2@cccc'],
        ['counter',0,'2@bbbb'],
        ['int',20,'2@aaaa']
      ])
      doc1.begin()
      doc1.inc("_root", "cnt", 5)
      doc1.commit()
      result = doc1.values("_root", "cnt")
      assert.deepEqual(result, [
        [ 'counter', 15, '2@cccc' ], [ 'counter', 5, '2@bbbb' ]
      ])
    })

    it('local inc increments all visible counters in a sequence', () => {
      let doc1 = Automerge.init("aaaa")
      doc1.begin()
      let seq = doc1.set("_root", "seq", LIST)
      doc1.insert(seq, 0, "hello")
      doc1.commit()
      let doc2 = Automerge.load(doc1.save(), "bbbb");
      let doc3 = Automerge.load(doc1.save(), "cccc");
      doc1.begin()
      doc1.set(seq, 0, 20)
      doc1.commit()
      doc2.begin()
      doc2.set(seq, 0, 0, "counter")
      doc2.commit()
      doc3.begin()
      doc3.set(seq, 0, 10, "counter")
      doc3.commit()
      doc1.applyChanges(doc2.getChanges(doc1.getHeads()))
      doc1.applyChanges(doc3.getChanges(doc1.getHeads()))
      let result = doc1.values(seq, 0)
      assert.deepEqual(result,[
        ['counter',10,'3@cccc'],
        ['counter',0,'3@bbbb'],
        ['int',20,'3@aaaa']
      ])
      doc1.begin()
      doc1.inc(seq, 0, 5)
      doc1.commit()
      result = doc1.values(seq, 0)
      assert.deepEqual(result, [
        [ 'counter', 15, '3@cccc' ], [ 'counter', 5, '3@bbbb' ]
      ])
    })
  })
})
