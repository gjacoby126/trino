/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.kaldb;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.USER_ERROR;

public enum KalDBErrorCode
        implements ErrorCodeSupplier
{
    KALDB_CONNECTION_ERROR(0, EXTERNAL),
    KALDB_INVALID_RESPONSE(1, EXTERNAL),
    KALDB_SSL_INITIALIZATION_FAILURE(2, EXTERNAL),
    KALDB_QUERY_FAILURE(3, USER_ERROR),
    KALDB_INVALID_METADATA(4, USER_ERROR);

    private final ErrorCode errorCode;

    KalDBErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0503_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
