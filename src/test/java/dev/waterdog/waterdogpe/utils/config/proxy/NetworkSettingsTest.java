/*
 * Copyright 2026 WaterdogTEAM
 * Licensed under the GNU General Public License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.waterdog.waterdogpe.utils.config.proxy;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NetworkSettingsTest {

    private static NetworkSettings withSessionTimeout(int seconds) throws ReflectiveOperationException {
        NetworkSettings settings = new NetworkSettings();
        Field field = NetworkSettings.class.getDeclaredField("sessionTimeout");
        field.setAccessible(true);
        field.setInt(settings, seconds);
        return settings;
    }

    @Test
    public void defaultMatchesThePreviousHardcodedValue() {
        // Servers that never set it behave exactly like the old hardcoded 10000L
        assertEquals(10_000L, new NetworkSettings().sessionTimeoutMillis());
    }

    @Test
    public void configuredSecondsBecomeMillis() throws ReflectiveOperationException {
        assertEquals(30_000L, withSessionTimeout(30).sessionTimeoutMillis());
    }

    @Test
    public void zeroOrNegativeFallsBackToDefault() throws ReflectiveOperationException {
        // Passing 0 through would make RakNet drop every session at once
        assertEquals(10_000L, withSessionTimeout(0).sessionTimeoutMillis());
        assertEquals(10_000L, withSessionTimeout(-5).sessionTimeoutMillis());
    }
}
