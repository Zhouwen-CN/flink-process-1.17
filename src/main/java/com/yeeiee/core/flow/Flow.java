package com.yeeiee.core.flow;

import com.yeeiee.core.context.Context;
import com.yeeiee.exception.BasicException;

public interface Flow {
    void run(Context context) throws BasicException;
}
