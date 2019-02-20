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

use crate::basic::{LogicalType, Repetition};
use crate::errors::ParquetError::General;
use crate::errors::Result;
use crate::schema::types::{Type, TypePtr};

pub trait TypeVisitor<R, C> {
    fn visit_primitive(&mut self, cur_type: TypePtr, context: C) -> Result<R>;

    /// Visit parquet list type.
    ///
    /// To fully understand this algorithm, please refer to
    /// [parquet doc](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md).
    fn visit_list(&mut self, cur_type: TypePtr, context: C) -> Result<R> {
        match cur_type.as_ref() {
            Type::PrimitiveType { .. } => panic!(
                "{:?} is a list type and can't be processed as primitive.",
                cur_type
            ),
            Type::GroupType {
                basic_info: _,
                fields,
            } if fields.len() == 1 => {
                let list_item = fields.first().unwrap();

                match list_item.as_ref() {
                    Type::PrimitiveType { .. } => {
                        if list_item.get_basic_info().repetition() == Repetition::REPEATED
                        {
                            self.visit_list_with_item(
                                cur_type.clone(),
                                list_item,
                                context,
                            )
                        } else {
                            Err(General(
                                "Primitive element type of list must be repeated."
                                    .to_string(),
                            ))
                        }
                    }
                    Type::GroupType {
                        basic_info: _,
                        fields,
                    } => {
                        if fields.len() == 1
                            && list_item.name() != "array"
                            && list_item.name() != format!("{}_tuple", cur_type.name())
                        {
                            self.visit_list_with_item(
                                cur_type.clone(),
                                fields.first().unwrap(),
                                context,
                            )
                        } else {
                            self.visit_list_with_item(
                                cur_type.clone(),
                                list_item,
                                context,
                            )
                        }
                    }
                }
            }
            _ => Err(General(
                "Group element type of list can only contain one field.".to_string(),
            )),
        }
    }
    fn visit_struct(&mut self, cur_type: TypePtr, context: C) -> Result<R>;
    fn visit_map(&mut self, cur_type: TypePtr, context: C) -> Result<R>;

    fn dispatch(&mut self, cur_type: TypePtr, context: C) -> Result<R> {
        if cur_type.is_primitive() {
            self.visit_primitive(cur_type, context)
        } else {
            match cur_type.get_basic_info().logical_type() {
                LogicalType::LIST => self.visit_list(cur_type, context),
                LogicalType::MAP | LogicalType::MAP_KEY_VALUE => {
                    self.visit_map(cur_type, context)
                }
                _ => self.visit_struct(cur_type, context),
            }
        }
    }

    fn visit_list_with_item(
        &mut self,
        list_type: TypePtr,
        item_type: &Type,
        context: C,
    ) -> Result<R>;
}
