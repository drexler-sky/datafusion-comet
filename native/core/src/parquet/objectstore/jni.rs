// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use bytes::Bytes;
use chrono::Utc;
use futures::{stream, stream::BoxStream, StreamExt};
use jni::{JNIEnv, JavaVM};
use jni::sys::jlong;
use once_cell::sync::OnceCell;

use object_store::{
    path::Path,
    GetOptions,
    GetResult,
    GetResultPayload,
    PutOptions,
    PutPayload,
    PutResult,
    MultipartUpload,
    PutMultipartOpts,
    ListResult,
    ObjectMeta,
    ObjectStore,
    Error as ObjectStoreError,
};

use std::collections::BTreeMap;
use std::ops::Range;
use object_store::Attributes;
use jni::objects::{JMap, JObject, JString};
use std::collections::HashMap;
use object_store::Error;
use jni::objects::JValue;

static JVM: OnceCell<JavaVM> = OnceCell::new();

pub fn init_jvm(env: &JNIEnv) {
    let _ = JVM.set(env.get_java_vm().expect("Failed to get JavaVM"));
}

fn get_jni_env<'a>() -> jni::AttachGuard<'a> {
    JVM.get()
        .expect("JVM not initialized")
        .attach_current_thread()
        .expect("Failed to attach thread")
}

/// Call Java Native.getLength(path)
pub fn call_get_length(
    raw_path: &str
) -> Result<usize, object_store::Error> {
    // TODO: relative path somehow doesn't work, prepend to get the absolute path for now. FIXME!!!
    let path = format!("s3a://test-bucket/{}", raw_path);
    println!("call_get_length path: {}", path);
    let mut env = match std::panic::catch_unwind(|| get_jni_env()) {
        Ok(env) => env,
        Err(_) => {
            eprintln!("[ERROR] get_jni_env() panicked");
            return Err(object_store::Error::Generic {
                store: "jni",
                source: "get_jni_env() panicked".into(),
            });
        }
    };

    println!("Successfully attached to JVM");
    let class = env
        .find_class("org/apache/comet/parquet/Native")
        .map_err(|e| object_store::Error::Generic {
            store: "jni",
            source: Box::new(e),
        })?;
    let jpath = env.new_string(&path).map_err(|e| object_store::Error::Generic {
        store: "jni",
        source: Box::new(e),
    })?;

    println!("Found Native class");

    // Create Java HashMap and populate it with config
    let result = env
        .call_static_method(
            class,
            "getLength",
            "(Ljava/lang/String;)J",  // ✅ CORRECT SIGNATURE
            &[jni::objects::JValue::Object(&jpath)],
        )
        .map_err(|e| object_store::Error::Generic {
            store: "jni",
            source: Box::new(e),
        })?
        .j()
        .unwrap_or(-1);

    if result < 0 {
        Err(object_store::Error::NotFound {
            path: path.to_string(),
            source: Box::new(Arc::new(std::io::Error::new(std::io::ErrorKind::NotFound, "not found"))),
        })
    } else {
        Ok(result as usize)
    }
}


/// Call Java Native.read(path, offset, len)
pub fn call_read(raw_path: &str, offset: usize, len: usize) -> Result<Vec<u8>, object_store::Error> {
    // TODO: relative path somehow doesn't work, prepend to get the absolute path for now. FIXME!!!
    let path = format!("s3a://test-bucket/{}", raw_path);
    println!("call_read: offset={}, len={}", offset, len);
    let mut env = get_jni_env();
    let class = env
        .find_class("org/apache/comet/parquet/Native")
        .map_err(|e| object_store::Error::Generic {
            store: "jni",
            source: Box::new(e),
        })?;
    let jpath = env.new_string(&path).map_err(|e| object_store::Error::Generic {
        store: "jni",
        source: Box::new(e),
    })?;

    let result = env
        .call_static_method(
            class,
            "read",
            "(Ljava/lang/String;JI)[B",
            &[(&jpath).into(), (offset as jlong).into(), (len as i32).into()],
        )
        .map_err(|e| object_store::Error::Generic {
            store: "jni",
            source: Box::new(e),
        })?
        .l()
        .map_err(|e| object_store::Error::Generic {
            store: "jni",
            source: Box::new(e),
        })?;

    let byte_array = jni::objects::JByteArray::from(result);
    if byte_array.is_null() {
        println!("byte_array is null.")
    }
    println!("byte_array: {:?}", byte_array);
    let output = env.convert_byte_array(byte_array).map_err(|e| object_store::Error::Generic {
        store: "jni",
        source: Box::new(e),
    })?;

    println!("call_read: returning {} bytes", output.len());
    Ok(output)
}


#[derive(Debug)]
pub struct JniObjectStore {
    configs: HashMap<String, String>,
}

impl JniObjectStore {
    pub fn new(configs: HashMap<String, String>) -> Self {
        Self { configs }
    }
}

impl std::fmt::Display for JniObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JniObjectStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for JniObjectStore {

    async fn get_opts(
        &self,
        location: &Path,
        _options: GetOptions,
    ) -> Result<GetResult, ObjectStoreError> {
        let path_str = location.to_string();
        let len = call_get_length(&path_str)?;
        println!("getLength returned: {}", len);
        let bytes = call_read(&path_str, 0, len)?;
        println!("read returned {} bytes", bytes.len());

        println!("Returned {} bytes for file {}", bytes.len(), path_str);
        println!("Footer trailer: {:?}", &bytes[bytes.len().saturating_sub(8)..]);

        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(stream::once(async move {
                Ok(Bytes::from(bytes))
            }))),
            meta: ObjectMeta {
                location: location.clone(),
                last_modified: Utc::now(),
                size: len as u64,
                version: None,
                e_tag: None,
            },
            range: 0..len as u64,
            attributes: Attributes::default(),
        })
    }

    async fn put_opts(
        &self,
        _location: &Path,
        _bytes: PutPayload,
        _opts: PutOptions,
    ) -> Result<PutResult, ObjectStoreError> {
        todo!()
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload + 'static>, ObjectStoreError> {
        todo!()
    }

    async fn delete(
        &self,
        _location: &Path,
    ) -> Result<(), ObjectStoreError> {
        todo!()
    }

    fn list(
        &self,
        _prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, ObjectStoreError>> {
        futures::stream::empty().boxed()
    }


    async fn list_with_delimiter(
        &self,
        _prefix: Option<&Path>,
    ) -> Result<ListResult, ObjectStoreError> {
        todo!()
    }

    async fn copy(
        &self,
        _from: &Path,
        _to: &Path,
    ) -> Result<(), ObjectStoreError> {
        todo!()
    }

    async fn copy_if_not_exists(
        &self,
        _from: &Path,
        _to: &Path,
    ) -> Result<(), ObjectStoreError> {
        todo!()
    }

    async fn head(
        &self,
        location: &Path,
    ) -> Result<ObjectMeta, ObjectStoreError> {
        let path = location.to_string();
        let len = call_get_length(&path)? as usize;
        Ok(ObjectMeta {
            location: location.clone(),
            last_modified: Utc::now(),
            size: len as u64,
            version: None,
            e_tag: None,
        })
    }
}
