###
# ‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
# Autonomic Proprietary 1.0
# ——————————————————————————————————————————————————————————————————————————————
# Copyright (C) 2018 Autonomic, LLC - All rights reserved
# ——————————————————————————————————————————————————————————————————————————————
# Proprietary and confidential.
#
# NOTICE:  All information contained herein is, and remains the property of
# Autonomic, LLC and its suppliers, if any.  The intellectual and technical
# concepts contained herein are proprietary to Autonomic, LLC and its suppliers
# and may be covered by U.S. and Foreign Patents, patents in process, and are
# protected by trade secret or copyright law. Dissemination of this information
# or reproduction of this material is strictly forbidden unless prior written
# permission is obtained from Autonomic, LLC.
#
# Unauthorized copy of this file, via any medium is strictly prohibited.
# ______________________________________________________________________________
###
./mvnw versions:set -DnewVersion=$FINAL_OR_BRANCH_SNAPSHOT_VERSION
./mvnw deploy
